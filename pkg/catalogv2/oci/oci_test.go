package oci

import (
	"fmt"
	"strings"
	"testing"

	v1 "github.com/rancher/rancher/pkg/apis/catalog.cattle.io/v1"
	"github.com/stretchr/testify/assert"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/repo"
)

func TestAddtoHelmRepoIndex(t *testing.T) {

	indexFile := repo.NewIndexFile()
	indexFile.Entries["testingchart"] = repo.ChartVersions{
		&repo.ChartVersion{
			Metadata: &chart.Metadata{
				Name:    "testingchart",
				Version: "0.1.0",
			},
			Digest: "digest",
		},
	}

	indexFile2 := repo.NewIndexFile()
	indexFile2.Entries["testingchart"] = repo.ChartVersions{
		&repo.ChartVersion{
			Metadata: &chart.Metadata{
				Name:    "testingchart",
				Version: "0.1.0",
			},
		},
	}

	tests := []struct {
		name                 string
		indexFile            *repo.IndexFile
		expectedErrMsg       string
		maxHelmRepoIndexSize int
	}{
		{
			"returns an error if indexFile size exceeds max size",
			repo.NewIndexFile(),
			"there are a lot of charts inside this oci",
			30,
		},
		{
			"adds the oci artifact to the helm repo index properly without deplication",
			indexFile2,
			"",
			30 * 1024 * 1024, // 30 MiB
		},
		{
			"avoids adding the oci artifact to the helm repo index if it already exists",
			indexFile,
			"",
			30 * 1024 * 1024, // 30 MiB
		},
		{
			"adds the oci artifact to the helm repo index properly",
			repo.NewIndexFile(),
			"",
			30 * 1024 * 1024, // 30 MiB
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := spinRegistry(0, true, true, tt.name, t)
			defer ts.Close()
			ociClient, err := NewClient(fmt.Sprintf("%s/testingchart:0.1.0", strings.Replace(ts.URL, "http", "oci", 1)), v1.RepoSpec{}, nil)
			assert.NoError(t, err)
			orasRepository, err := ociClient.GetOrasRepository()
			orasRepository.PlainHTTP = true
			assert.NoError(t, err)

			maxHelmRepoIndexSize = tt.maxHelmRepoIndexSize
			err = addToHelmRepoIndex(*ociClient, tt.indexFile, orasRepository)
			if tt.expectedErrMsg != "" {
				assert.Contains(t, err.Error(), tt.expectedErrMsg)
			} else {
				assert.NoError(t, err)
				if len(tt.indexFile.Entries) > 0 {
					assert.Equal(t, len(tt.indexFile.Entries["testingchart"]), 1)
				}
			}
		})
	}
}
