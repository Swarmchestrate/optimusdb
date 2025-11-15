package contextualmetadata

import (
	"strings"
)

// DomainVocabulary holds domain-specific terminology
type DomainVocabulary struct {
	Domain   string
	Keywords []string
	Synonyms map[string][]string
}

// GetRenewableEnergyVocabulary returns renewable energy domain vocabulary
func GetRenewableEnergyVocabulary() *DomainVocabulary {
	return &DomainVocabulary{
		Domain: "renewable_energy",
		Keywords: []string{
			// General
			"renewable_energy", "clean_energy", "green_energy", "power_generation",
			"energy_production", "capacity", "efficiency", "availability",

			// Solar
			"photovoltaic", "PV", "solar_panel", "solar_array", "inverter",
			"irradiance", "GHI", "DNI", "DHI", "solar_radiation",
			"sun_elevation", "azimuth", "tracking", "fixed_tilt",
			"DC_power", "AC_power", "MPPT", "string_voltage",

			// Wind
			"wind_turbine", "wind_farm", "nacelle", "rotor", "blade",
			"wind_speed", "wind_direction", "yaw", "pitch",
			"cut_in_speed", "rated_speed", "cut_out_speed",
			"power_curve", "capacity_factor",

			// Grid
			"grid_connection", "grid_frequency", "voltage", "current",
			"active_power", "reactive_power", "power_factor",
			"grid_synchronization", "frequency_regulation",

			// Environmental
			"temperature", "humidity", "pressure", "precipitation",
			"cloud_cover", "weather_conditions",

			// Operational
			"availability", "downtime", "maintenance", "fault",
			"alarm", "warning", "status", "state",
			"SCADA", "monitoring", "telemetry", "sensor",

			// Performance
			"performance_ratio", "yield", "losses", "degradation",
			"soiling", "shading", "curtailment",
		},
		Synonyms: map[string][]string{
			"power":       {"energy", "electricity", "generation"},
			"turbine":     {"generator", "wind_generator"},
			"solar_panel": {"PV_module", "photovoltaic_panel"},
			"inverter":    {"converter", "DC_AC_converter"},
			"temperature": {"temp", "thermal"},
		},
	}
}

// GetSolarVocabulary returns solar-specific vocabulary
func GetSolarVocabulary() *DomainVocabulary {
	return &DomainVocabulary{
		Domain: "solar",
		Keywords: []string{
			"PV_module", "solar_cell", "crystalline_silicon", "thin_film",
			"module_temperature", "cell_temperature", "NOCT",
			"short_circuit_current", "open_circuit_voltage",
			"fill_factor", "efficiency", "degradation_rate",
			"bifacial", "monofacial", "tracking_system",
			"single_axis", "dual_axis", "fixed_tilt",
		},
	}
}

// GetWindVocabulary returns wind-specific vocabulary
func GetWindVocabulary() *DomainVocabulary {
	return &DomainVocabulary{
		Domain: "wind",
		Keywords: []string{
			"wind_class", "turbulence_intensity", "wind_shear",
			"hub_height", "rotor_diameter", "swept_area",
			"tip_speed_ratio", "blade_pitch_angle", "yaw_error",
			"gearbox", "generator", "transformer",
			"offshore", "onshore", "capacity_factor",
		},
	}
}

// EnrichWithVocabulary enhances metadata with domain vocabulary
func EnrichWithVocabulary(metadata map[string]interface{}, domain string) map[string]interface{} {
	var vocab *DomainVocabulary

	domainLower := strings.ToLower(domain)
	switch {
	case strings.Contains(domainLower, "solar"):
		vocab = GetSolarVocabulary()
	case strings.Contains(domainLower, "wind"):
		vocab = GetWindVocabulary()
	default:
		vocab = GetRenewableEnergyVocabulary()
	}

	// Add domain keywords if not present
	if tags, ok := metadata["tags"].([]string); ok {
		enrichedTags := enrichTags(tags, vocab)
		metadata["tags"] = enrichedTags
	}

	// Add domain field
	metadata["domain"] = vocab.Domain
	metadata["domain_keywords"] = vocab.Keywords

	return metadata
}

// enrichTags adds relevant domain keywords to tags
func enrichTags(tags []string, vocab *DomainVocabulary) []string {
	tagSet := make(map[string]bool)
	for _, tag := range tags {
		tagSet[strings.ToLower(tag)] = true
	}

	// Add relevant domain keywords that match existing tags
	for _, keyword := range vocab.Keywords {
		keywordLower := strings.ToLower(keyword)

		// Check if keyword or its synonyms appear in tags
		if tagSet[keywordLower] {
			continue
		}

		// Check synonyms
		for originalTerm, synonyms := range vocab.Synonyms {
			if tagSet[strings.ToLower(originalTerm)] {
				for _, synonym := range synonyms {
					if strings.ToLower(synonym) == keywordLower {
						tags = append(tags, keyword)
						break
					}
				}
			}
		}
	}

	return tags
}

// InferDomain attempts to infer domain from column names and values
func InferDomain(profile *DatasetProfile) string {
	solarScore := 0
	windScore := 0

	solarTerms := []string{"solar", "pv", "irradiance", "inverter", "panel"}
	windTerms := []string{"wind", "turbine", "rotor", "blade", "nacelle"}

	// Check column names
	for _, col := range profile.Profiles {
		nameLower := strings.ToLower(col.Name)

		for _, term := range solarTerms {
			if strings.Contains(nameLower, term) {
				solarScore++
			}
		}

		for _, term := range windTerms {
			if strings.Contains(nameLower, term) {
				windScore++
			}
		}
	}

	if solarScore > windScore {
		return "solar"
	} else if windScore > solarScore {
		return "wind"
	}

	return "renewable_energy"
}
