package commit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/argoproj/argo-cd/v3/commitserver/apiclient"
	"github.com/argoproj/argo-cd/v3/commitserver/metrics"
	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/util/git"
	"github.com/argoproj/argo-cd/v3/util/io/files"
)

// Service is the service that handles commit requests.
type Service struct {
	gitCredsStore     git.CredsStore
	metricsServer     *metrics.Server
	repoClientFactory RepoClientFactory
}

// NewService returns a new instance of the commit service.
func NewService(gitCredsStore git.CredsStore, metricsServer *metrics.Server) *Service {
	return &Service{
		gitCredsStore:     gitCredsStore,
		metricsServer:     metricsServer,
		repoClientFactory: NewRepoClientFactory(gitCredsStore, metricsServer),
	}
}

// CommitHydratedManifests handles a commit request. It clones the repository, checks out the sync branch, checks out
// the target branch, clears the repository contents, writes the manifests to the repository, commits the changes, and
// pushes the changes. It returns the hydrated revision SHA and an error if one occurred.
func (s *Service) CommitHydratedManifests(_ context.Context, r *apiclient.CommitHydratedManifestsRequest) (*apiclient.CommitHydratedManifestsResponse, error) {
	// This method is intentionally short. It's a wrapper around handleCommitRequest that adds metrics and logging.
	// Keep logic here minimal and put most of the logic in handleCommitRequest.
	startTime := time.Now()

	// We validate for a nil repo in handleCommitRequest, but we need to check for a nil repo here to get the repo URL
	// for metrics.
	var repoURL string
	if r.DestRepo != nil {
		repoURL = r.DestRepo.Repo
	}

	var err error
	s.metricsServer.IncPendingCommitRequest(repoURL)
	defer func() {
		s.metricsServer.DecPendingCommitRequest(repoURL)
		commitResponseType := metrics.CommitResponseTypeSuccess
		if err != nil {
			commitResponseType = metrics.CommitResponseTypeFailure
		}
		s.metricsServer.IncCommitRequest(repoURL, commitResponseType)
		s.metricsServer.ObserveCommitRequestDuration(repoURL, commitResponseType, time.Since(startTime))
	}()

	logCtx := log.WithFields(log.Fields{
		"branch":     r.TargetBranch,
		"drySHA":     r.DrySha,
		"sourceRepo": r.SourceRepo.Repo,
		"destRepo":   r.DestRepo.Repo,
	})

	out, sha, err := s.handleCommitRequest(logCtx, r)
	if err != nil {
		logCtx.WithError(err).WithField("output", out).Error("failed to handle commit request")

		// No need to wrap this error, sufficient context is build in handleCommitRequest.
		return &apiclient.CommitHydratedManifestsResponse{}, err
	}

	logCtx.Info("Successfully handled commit request")
	return &apiclient.CommitHydratedManifestsResponse{
		HydratedSha: sha,
	}, nil
}

// handleCommitRequest handles the commit request. It clones the repository, checks out the sync branch, checks out the
// target branch, clears the repository contents, writes the manifests to the repository, commits the changes, and pushes
// the changes. It returns the output of the git commands and an error if one occurred.
func (s *Service) handleCommitRequest(logCtx *log.Entry, r *apiclient.CommitHydratedManifestsRequest) (string, string, error) {
	if r.SourceRepo == nil {
		return "", "", errors.New("source repo is required")
	}
	if r.SourceRepo.Repo == "" {
		return "", "", errors.New("source repo URL is required")
	}
	if r.DestRepo == nil {
		return "", "", errors.New("destination repo is required")
	}
	if r.DestRepo.Repo == "" {
		return "", "", errors.New("destination repo URL is required")
	}
	if r.TargetBranch == "" {
		return "", "", errors.New("target branch is required")
	}
	if r.SyncBranch == "" {
		return "", "", errors.New("sync branch is required")
	}

	logCtx = logCtx.WithFields(log.Fields{
		"sourceRepo": r.SourceRepo.Repo,
		"destRepo":   r.DestRepo.Repo,
	})
	logCtx.Debug("Initiating git clients")

	// Initialize source repo client
	logCtx.WithField("sourceRepo", r.SourceRepo.Repo).Debug("Creating source temp directory")
	sourceDirPath, err := files.CreateTempDir("/tmp/_commit-service-source")
	if err != nil {
		logCtx.WithError(err).Error("Failed to create source temp directory")
		return "", "", fmt.Errorf("failed to create source temp dir: %w", err)
	}
	logCtx.WithField("sourceDirPath", sourceDirPath).Debug("Created source temp directory")

	logCtx.Debug("Initializing source git client")
	_, sourceCleanup, err := s.initGitClient(logCtx, r.SourceRepo, sourceDirPath)
	if err != nil {
		logCtx.WithError(err).WithField("sourceDirPath", sourceDirPath).Error("Failed to initialize source git client")
		return "", "", fmt.Errorf("failed to initialize source git client: %w", err)
	}
	logCtx.Debug("Successfully initialized source git client")
	defer sourceCleanup()

	// Initialize destination repo client
	destDirPath, err := files.CreateTempDir("/tmp/_commit-service-dest")
	if err != nil {
		return "", "", fmt.Errorf("failed to create destination temp dir: %w", err)
	}
	destGitClient, destCleanup, err := s.initGitClient(logCtx, r.DestRepo, destDirPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to initialize destination git client: %w", err)
	}
	defer destCleanup()

	logCtx.Debugf("Checking out target branch %s", r.TargetBranch)
	var out string
	out, err = destGitClient.CheckoutOrOrphan(r.TargetBranch, false)
	if err != nil {
		return out, "", fmt.Errorf("failed to checkout target branch: %w", err)
	}

	logCtx.Debug("Clearing destination repo contents")
	out, err = destGitClient.RemoveContents()
	if err != nil {
		return out, "", fmt.Errorf("failed to clear destination repo: %w", err)
	}

	logCtx.Debug("Writing manifests")
	err = WriteForPaths(destDirPath, r.DestRepo.Repo, r.DrySha, r.Paths)
	if err != nil {
		logCtx.WithError(err).WithField("destDirPath", destDirPath).Error("Failed to write manifests")
		return "", "", fmt.Errorf("failed to write manifests: %w", err)
	}

	logCtx.WithFields(log.Fields{
		"targetBranch":  r.TargetBranch,
		"commitMessage": r.CommitMessage,
	}).Debug("Committing and pushing changes")
	out, err = destGitClient.CommitAndPush(r.TargetBranch, r.CommitMessage)
	if err != nil {
		logCtx.WithError(err).WithField("output", out).Error("Failed to commit and push changes")
		return out, "", fmt.Errorf("failed to commit and push: %w", err)
	}

	logCtx.Debug("Getting commit SHA")
	sha, err := destGitClient.CommitSHA()
	if err != nil {
		logCtx.WithError(err).Error("Failed to get commit SHA")
		return "", "", fmt.Errorf("failed to get commit SHA: %w", err)
	}

	logCtx.WithField("hydratedSHA", sha).Info("Successfully completed commit process")
	return "", sha, nil
}

// initGitClient initializes a git client for the given repository and returns the client, a cleanup function that should be called when the directory is no longer needed, and an error if one occurred.
func (s *Service) initGitClient(logCtx *log.Entry, repo *v1alpha1.Repository, dirPath string) (git.Client, func(), error) {
	// Call cleanupOrLog in this function if an error occurs to ensure the temp dir is cleaned up.
	cleanupOrLog := func() {
		err := os.RemoveAll(dirPath)
		if err != nil {
			logCtx.WithError(err).Error("failed to cleanup temp dir")
		}
	}

	gitClient, err := s.repoClientFactory.NewClient(repo, dirPath)
	if err != nil {
		cleanupOrLog()
		return nil, nil, fmt.Errorf("failed to create git client: %w", err)
	}

	logCtx.Debugf("Initializing repo %s", repo.Repo)
	err = gitClient.Init()
	if err != nil {
		cleanupOrLog()
		return nil, nil, fmt.Errorf("failed to init git client: %w", err)
	}

	logCtx.Debugf("Fetching repo %s", repo.Repo)
	err = gitClient.Fetch("")
	if err != nil {
		cleanupOrLog()
		return nil, nil, fmt.Errorf("failed to clone repo: %w", err)
	}

	// FIXME: make it work for GHE
	// logCtx.Debugf("Getting user info for repo credentials")
	// gitCreds := repo.GetGitCreds(s.gitCredsStore)
	// startTime := time.Now()
	// authorName, authorEmail, err := gitCreds.GetUserInfo(ctx)
	// s.metricsServer.ObserveUserInfoRequestDuration(repo.Repo, getCredentialType(repo), time.Since(startTime))
	// if err != nil {
	//	 cleanupOrLog()
	//	 return nil, nil, fmt.Errorf("failed to get github app info: %w", err)
	// }
	var authorName, authorEmail string

	if authorName == "" {
		authorName = "Argo CD"
	}
	if authorEmail == "" {
		logCtx.Warnf("Author email not available, using 'argo-cd@example.com'.")
		authorEmail = "argo-cd@example.com"
	}

	logCtx.Debugf("Setting author %s <%s>", authorName, authorEmail)
	_, err = gitClient.SetAuthor(authorName, authorEmail)
	if err != nil {
		cleanupOrLog()
		return nil, nil, fmt.Errorf("failed to set author: %w", err)
	}

	return gitClient, cleanupOrLog, nil
}

type hydratorMetadataFile struct {
	RepoURL  string   `json:"repoURL"`
	DrySHA   string   `json:"drySha"`
	Commands []string `json:"commands"`
}

// TODO: make this configurable via ConfigMap.
var manifestHydrationReadmeTemplate = `
# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

` + "```shell\n" + `
git clone {{ .RepoURL }}
# cd into the cloned directory
git checkout {{ .DrySHA }}
{{ range $command := .Commands -}}
{{ $command }}
{{ end -}}` + "```"
