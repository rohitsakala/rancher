package notifications

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKnownCategoriesValid(t *testing.T) {
	for name, cat := range knownCategories {
		assert.Equal(t, name, cat.Category, "map key must match Category field")
		assert.NotEmpty(t, cat.EventReasons, "EventReasons must not be empty for %s", name)
	}
}

func TestKnownCategoriesCertExpiry(t *testing.T) {
	cat, ok := knownCategories["cert-expiry"]
	assert.True(t, ok, "cert-expiry must be a known category")
	assert.Contains(t, cat.EventReasons, "CertificateExpirationWarning")
	assert.Contains(t, cat.EventReasons, "CACertificateExpirationWarning")
}

func TestReasonToCategoryLookup(t *testing.T) {
	cat, ok := reasonToCategory["CertificateExpirationWarning"]
	assert.True(t, ok)
	assert.Equal(t, "cert-expiry", cat.Category)

	cat, ok = reasonToCategory["CACertificateExpirationWarning"]
	assert.True(t, ok)
	assert.Equal(t, "cert-expiry", cat.Category)

	_, ok = reasonToCategory["UnknownReason"]
	assert.False(t, ok)
}
