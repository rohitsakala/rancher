package notifications

import (
	"testing"
	"time"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	mgmtcontrollers "github.com/rancher/rancher/pkg/generated/controllers/management.cattle.io/v3"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

var certExpiry = knownCategories["cert-expiry"]

type mockNotificationClient struct {
	created map[string]*v3.RancherUserNotification
	updated map[string]*v3.RancherUserNotification
	deleted map[string]bool
}

func newMockNotificationClient() *mockNotificationClient {
	return &mockNotificationClient{
		created: make(map[string]*v3.RancherUserNotification),
		updated: make(map[string]*v3.RancherUserNotification),
		deleted: make(map[string]bool),
	}
}

func (m *mockNotificationClient) Create(obj *v3.RancherUserNotification) (*v3.RancherUserNotification, error) {
	m.created[obj.Name] = obj.DeepCopy()
	return obj, nil
}

func (m *mockNotificationClient) Update(obj *v3.RancherUserNotification) (*v3.RancherUserNotification, error) {
	m.updated[obj.Name] = obj.DeepCopy()
	return obj, nil
}

func (m *mockNotificationClient) UpdateStatus(obj *v3.RancherUserNotification) (*v3.RancherUserNotification, error) {
	return obj, nil
}

func (m *mockNotificationClient) Delete(name string, options *metav1.DeleteOptions) error {
	m.deleted[name] = true
	return nil
}

func (m *mockNotificationClient) Get(name string, options metav1.GetOptions) (*v3.RancherUserNotification, error) {
	return nil, nil
}

func (m *mockNotificationClient) List(opts metav1.ListOptions) (*v3.RancherUserNotificationList, error) {
	return &v3.RancherUserNotificationList{}, nil
}

func (m *mockNotificationClient) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (m *mockNotificationClient) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (*v3.RancherUserNotification, error) {
	return nil, nil
}

func (m *mockNotificationClient) WithImpersonation(impersonate rest.ImpersonationConfig) (generic.NonNamespacedClientInterface[*v3.RancherUserNotification, *v3.RancherUserNotificationList], error) {
	return nil, nil
}

var _ mgmtcontrollers.RancherUserNotificationClient = (*mockNotificationClient)(nil)

type mockNotificationCache struct {
	items map[string]*v3.RancherUserNotification
}

func newMockNotificationCache() *mockNotificationCache {
	return &mockNotificationCache{
		items: make(map[string]*v3.RancherUserNotification),
	}
}

func (m *mockNotificationCache) Get(name string) (*v3.RancherUserNotification, error) {
	if n, ok := m.items[name]; ok {
		return n, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "management.cattle.io", Resource: "rancherusernotifications"}, name)
}

func (m *mockNotificationCache) List(selector labels.Selector) ([]*v3.RancherUserNotification, error) {
	var result []*v3.RancherUserNotification
	for _, n := range m.items {
		if selector.Matches(labels.Set(n.Labels)) {
			result = append(result, n)
		}
	}
	return result, nil
}

func (m *mockNotificationCache) AddIndexer(indexName string, indexer generic.Indexer[*v3.RancherUserNotification]) {
}

func (m *mockNotificationCache) GetByIndex(indexName, key string) ([]*v3.RancherUserNotification, error) {
	return nil, nil
}

var _ mgmtcontrollers.RancherUserNotificationCache = (*mockNotificationCache)(nil)

func TestParseEnabledCategories(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{name: "single known category", input: "cert-expiry", expected: []string{"cert-expiry"}},
		{name: "empty string", input: "", expected: nil},
		{name: "unknown category ignored", input: "cert-expiry,unknown-thing", expected: []string{"cert-expiry"}},
		{name: "whitespace trimmed", input: " cert-expiry , ", expected: []string{"cert-expiry"}},
		{name: "all unknown", input: "foo,bar", expected: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseEnabledCategories(tt.input)
			var names []string
			for _, c := range result {
				names = append(names, c.Category)
			}
			assert.Equal(t, tt.expected, names)
		})
	}
}

func TestIsCategoryEnabled(t *testing.T) {
	assert.True(t, isCategoryEnabled("cert-expiry", "cert-expiry"))
	assert.True(t, isCategoryEnabled("cert-expiry,oom-kill", "cert-expiry"))
	assert.True(t, isCategoryEnabled(" cert-expiry , oom-kill ", "cert-expiry"))
	assert.False(t, isCategoryEnabled("oom-kill", "cert-expiry"))
	assert.False(t, isCategoryEnabled("", "cert-expiry"))
}

func TestNotificationName(t *testing.T) {
	event := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-1",
			Kind:      "Node",
		},
		Reason: "CertificateExpirationWarning",
	}

	name := notificationName("cert-expiry", "c-m-abc123", event)
	assert.True(t, len(name) <= 253)
	assert.Contains(t, name, "cert-expiry-c-m-abc123-")

	name2 := notificationName("cert-expiry", "c-m-abc123", event)
	assert.Equal(t, name, name2, "same event should produce same name")

	event2 := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-2",
			Kind:      "Node",
		},
		Reason: "CertificateExpirationWarning",
	}
	name3 := notificationName("cert-expiry", "c-m-abc123", event2)
	assert.NotEqual(t, name, name3, "different events should produce different names")
}

func TestNotificationNameDifferentCategories(t *testing.T) {
	event := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-1",
			Kind:      "Node",
		},
		Reason: "SomeReason",
	}

	name1 := notificationName("cert-expiry", "c-m-test", event)
	name2 := notificationName("other-category", "c-m-test", event)
	assert.NotEqual(t, name1, name2, "different categories should produce different names")
}

func TestMapSeverity(t *testing.T) {
	assert.Equal(t, "warning", mapSeverity(&corev1.Event{Type: corev1.EventTypeWarning}))
	assert.Equal(t, "info", mapSeverity(&corev1.Event{Type: corev1.EventTypeNormal}))
}

func TestBuildDetails(t *testing.T) {
	event := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-1",
			Kind:      "Node",
		},
		Reason:  "CertificateExpirationWarning",
		Message: "Certificate will expire soon",
		Source:  corev1.EventSource{Component: "rke2-cert-monitor"},
		Count:   3,
		LastTimestamp: metav1.Time{
			Time: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	details := buildDetails(event)
	assert.Equal(t, "CertificateExpirationWarning", details["eventReason"])
	assert.Equal(t, "node-1", details["involvedObjectName"])
	assert.Equal(t, "kube-system", details["involvedObjectNamespace"])
	assert.Equal(t, "Node", details["involvedObjectKind"])
	assert.Equal(t, "rke2-cert-monitor", details["sourceComponent"])
	assert.Equal(t, "3", details["count"])
	assert.Equal(t, "Certificate will expire soon", details["eventMessage"])
	assert.NotEmpty(t, details["lastSeen"])
}

func TestCreateOrUpdateNotification_Create(t *testing.T) {
	client := newMockNotificationClient()
	cache := newMockNotificationCache()
	c := &controller{
		clusterName:       "c-m-test",
		notifications:     client,
		notificationCache: cache,
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "test-event"},
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-1",
			Kind:      "Node",
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "CertificateExpirationWarning",
		Message: "Certificate will expire in 30 days",
		Source:  corev1.EventSource{Component: "rke2-cert-monitor"},
	}

	err := c.createOrUpdateNotification("cert-expiry-c-m-test-abc", event, certExpiry)
	assert.NoError(t, err)
	assert.Len(t, client.created, 1)

	created := client.created["cert-expiry-c-m-test-abc"]
	assert.Equal(t, "rke2-cert-monitor", created.ComponentName)
	assert.Equal(t, "warning", created.Severity)
	assert.Equal(t, certExpiry.Category, created.Category)
	assert.Equal(t, "c-m-test", created.ClusterName)
	assert.Equal(t, "Certificate will expire in 30 days", created.Message)
}

func TestCreateOrUpdateNotification_Update(t *testing.T) {
	client := newMockNotificationClient()
	cache := newMockNotificationCache()
	cache.items["cert-expiry-c-m-test-abc"] = &v3.RancherUserNotification{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cert-expiry-c-m-test-abc",
			Labels: map[string]string{
				categoryLabel:    certExpiry.Category,
				clusterNameLabel: "c-m-test",
			},
		},
		Severity: "warning",
		Message:  "Old message",
	}

	c := &controller{
		clusterName:       "c-m-test",
		notifications:     client,
		notificationCache: cache,
	}

	event := &corev1.Event{
		Type:    corev1.EventTypeWarning,
		Reason:  "CertificateExpirationWarning",
		Message: "New message",
	}

	err := c.createOrUpdateNotification("cert-expiry-c-m-test-abc", event, certExpiry)
	assert.NoError(t, err)
	assert.Len(t, client.created, 0)
	assert.Len(t, client.updated, 1)
	assert.Equal(t, "New message", client.updated["cert-expiry-c-m-test-abc"].Message)
}

func TestCreateOrUpdateNotification_NoUpdateIfUnchanged(t *testing.T) {
	client := newMockNotificationClient()
	cache := newMockNotificationCache()
	cache.items["cert-expiry-c-m-test-abc"] = &v3.RancherUserNotification{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cert-expiry-c-m-test-abc",
			Labels: map[string]string{
				categoryLabel:    certExpiry.Category,
				clusterNameLabel: "c-m-test",
			},
		},
		Severity: "warning",
		Message:  "Same message",
	}

	c := &controller{
		clusterName:       "c-m-test",
		notifications:     client,
		notificationCache: cache,
	}

	event := &corev1.Event{
		Type:    corev1.EventTypeWarning,
		Reason:  "CertificateExpirationWarning",
		Message: "Same message",
	}

	err := c.createOrUpdateNotification("cert-expiry-c-m-test-abc", event, certExpiry)
	assert.NoError(t, err)
	assert.Len(t, client.created, 0)
	assert.Len(t, client.updated, 0, "should not update if nothing changed")
}

func TestOnEventChange_NilEvent(t *testing.T) {
	c := &controller{clusterName: "c-m-test"}
	result, err := c.onEventChange("some-key", nil)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestOnEventChange_UnknownReason(t *testing.T) {
	c := &controller{clusterName: "c-m-test"}
	event := &corev1.Event{Reason: "SomeUnknownReason"}
	result, err := c.onEventChange("some-key", event)
	assert.NoError(t, err)
	assert.Equal(t, event, result)
}

func TestOnEventChange_KnownReason(t *testing.T) {
	client := newMockNotificationClient()
	cache := newMockNotificationCache()
	c := &controller{
		clusterName:       "c-m-test",
		notifications:     client,
		notificationCache: cache,
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "test-event"},
		InvolvedObject: corev1.ObjectReference{
			Namespace: "kube-system",
			Name:      "node-1",
			Kind:      "Node",
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "CertificateExpirationWarning",
		Message: "Certificate will expire in 30 days",
	}

	result, err := c.onEventChange("kube-system/test-event", event)
	assert.NoError(t, err)
	assert.Equal(t, event, result)
	assert.Len(t, client.created, 1)
}

func TestCleanupAcknowledgedNotifications(t *testing.T) {
	client := newMockNotificationClient()
	cache := newMockNotificationCache()

	oldAckTime := metav1.NewTime(time.Now().Add(-8 * 24 * time.Hour))
	recentAckTime := metav1.NewTime(time.Now().Add(-1 * 24 * time.Hour))

	cache.items["cert-expiry-c-m-test-old"] = &v3.RancherUserNotification{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cert-expiry-c-m-test-old",
			Labels: map[string]string{
				categoryLabel:    certExpiry.Category,
				clusterNameLabel: "c-m-test",
			},
		},
		AcknowledgedAt: &oldAckTime,
	}
	cache.items["cert-expiry-c-m-test-recent"] = &v3.RancherUserNotification{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cert-expiry-c-m-test-recent",
			Labels: map[string]string{
				categoryLabel:    certExpiry.Category,
				clusterNameLabel: "c-m-test",
			},
		},
		AcknowledgedAt: &recentAckTime,
	}
	cache.items["cert-expiry-c-m-test-unacked"] = &v3.RancherUserNotification{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cert-expiry-c-m-test-unacked",
			Labels: map[string]string{
				categoryLabel:    certExpiry.Category,
				clusterNameLabel: "c-m-test",
			},
		},
		AcknowledgedAt: nil,
	}

	c := &controller{
		clusterName:       "c-m-test",
		notifications:     client,
		notificationCache: cache,
	}

	c.cleanupAcknowledgedNotifications(certExpiry)

	assert.True(t, client.deleted["cert-expiry-c-m-test-old"], "old acknowledged notification should be deleted")
	assert.False(t, client.deleted["cert-expiry-c-m-test-recent"], "recently acknowledged notification should not be deleted")
	assert.False(t, client.deleted["cert-expiry-c-m-test-unacked"], "unacknowledged notification should not be deleted")
}
