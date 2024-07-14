package bigquerybackup

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetBigQueryClient(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		projectID string
		wantErr   bool
	}{
		{
			name:      "Valid project ID",
			projectID: "test-project",
			wantErr:   false,
		},
		{
			name:      "Empty project ID",
			projectID: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bp := &backupParams{
				projectID: tt.projectID,
			}
			err := bp.setBigQueryClient(ctx)
			if (err != nil) && tt.wantErr {
				t.Errorf("setBigQueryClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}

}
func TestSetBigQueryClientConcurrency(t *testing.T) {
	ctx := context.Background()
	bp := &backupParams{
		projectID: "test-project",
	}

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := bp.setBigQueryClient(ctx)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestSetProjectID(t *testing.T) {
	tests := []struct {
		name    string
		envVar  string
		wantErr bool
	}{
		{
			name:    "Valid project ID",
			envVar:  "test-project",
			wantErr: false,
		},
		{
			name:    "Empty project ID",
			envVar:  "",
			wantErr: true,
		},
		{
			name:    "Whitespace project ID",
			envVar:  "   ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("GCP_PROJECT", tt.envVar)
			defer os.Unsetenv("GCP_PROJECT")

			bp := &backupParams{}
			err := bp.setProjectID()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.envVar, bp.projectID)
			}
		})
	}
}

func TestSetProjectIDEnvironmentChange(t *testing.T) {
	initialProject := "initial-project"
	updatedProject := "updated-project"

	os.Setenv("GCP_PROJECT", initialProject)
	bp1 := &backupParams{}
	err1 := bp1.setProjectID()
	assert.NoError(t, err1)
	assert.Equal(t, initialProject, bp1.projectID)

	os.Setenv("GCP_PROJECT", updatedProject)
	bp2 := &backupParams{}
	err2 := bp2.setProjectID()
	assert.NoError(t, err2)
	assert.Equal(t, updatedProject, bp2.projectID)

	os.Unsetenv("GCP_PROJECT")
}

// TODO: Add additional tests
