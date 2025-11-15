package contextualmetadata

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProfileColumn(t *testing.T) {
	data := [][]string{
		{"123", "456"},
		{"789", "101"},
		{"", "202"},
	}

	profile := profileColumn("test_col", data, 0, 3)

	assert.Equal(t, "test_col", profile.Name)
	assert.Equal(t, "int", profile.InferredType)
	assert.InDelta(t, 0.33, profile.NullRatio, 0.01)
	assert.Equal(t, 2, profile.Cardinality)
}

func TestInferType(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"integers", []string{"1", "2", "3"}, "int"},
		{"floats", []string{"1.5", "2.3", "3.7"}, "float"},
		{"dates", []string{"2024-01-01", "2024-01-02"}, "date"},
		{"strings", []string{"abc", "def", "ghi"}, "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferType(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDomainInference(t *testing.T) {
	profile := &DatasetProfile{
		Profiles: []ColumnProfile{
			{Name: "solar_irradiance"},
			{Name: "panel_temperature"},
			{Name: "power_output"},
		},
	}

	domain := InferDomain(profile)
	assert.Equal(t, "solar", domain)
}
