package notifications

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	mgmtcontrollers "github.com/rancher/rancher/pkg/generated/controllers/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/settings"
	"github.com/rancher/rancher/pkg/types/config"
	wranglercorev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/ticker"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	gcInterval        = 6 * time.Hour
	acknowledgeMaxAge = 7 * 24 * time.Hour
	categoryLabel     = "cattle.io/notification-category"
	clusterNameLabel  = "cattle.io/cluster-name"
)

type controller struct {
	ctx               context.Context
	clusterName       string
	notifications     mgmtcontrollers.RancherUserNotificationClient
	notificationCache mgmtcontrollers.RancherUserNotificationCache
}

func Register(ctx context.Context, cluster *config.UserContext) {
	coreControllers := wranglercorev1.New(cluster.ControllerFactory)

	c := &controller{
		ctx:               ctx,
		clusterName:       cluster.ClusterName,
		notifications:     cluster.Management.Wrangler.Mgmt.RancherUserNotification(),
		notificationCache: cluster.Management.Wrangler.Mgmt.RancherUserNotification().Cache(),
	}

	coreControllers.Event().OnChange(ctx, "notification-event-watcher", c.onEventChange)
	go c.runGC(ctx)
}

func (c *controller) onEventChange(_ string, event *corev1.Event) (*corev1.Event, error) {
	if event == nil {
		return nil, nil
	}

	cat, ok := reasonToCategory[event.Reason]
	if !ok {
		return event, nil
	}

	if !isCategoryEnabled(settings.UserNotificationEventTypes.Get(), cat.Category) {
		return event, nil
	}

	name := notificationName(cat.Category, c.clusterName, event)
	if err := c.createOrUpdateNotification(name, event, cat); err != nil {
		logrus.Errorf("[notification-event-watcher] error creating notification %s: %v", name, err)
		return event, err
	}

	return event, nil
}

func isCategoryEnabled(raw, category string) bool {
	for _, name := range strings.Split(raw, ",") {
		if strings.TrimSpace(name) == category {
			return true
		}
	}
	return false
}

func parseEnabledCategories(raw string) []EventCategory {
	var result []EventCategory
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cat, ok := knownCategories[name]
		if !ok {
			logrus.Warnf("[notification-event-watcher] unknown notification category %q, ignoring", name)
			continue
		}
		result = append(result, cat)
	}
	return result
}

func (c *controller) createOrUpdateNotification(name string, event *corev1.Event, cat EventCategory) error {
	severity := mapSeverity(event)
	details := buildDetails(event)

	existing, err := c.notificationCache.Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if existing == nil {
		_, err = c.notifications.Create(&v3.RancherUserNotification{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					categoryLabel:    cat.Category,
					clusterNameLabel: c.clusterName,
				},
			},
			ComponentName: event.Source.Component,
			Message:       event.Message,
			Severity:      severity,
			Category:      cat.Category,
			ClusterName:   c.clusterName,
			EventDetails:  details,
		})
		return err
	}

	if existing.Severity != severity || existing.Message != event.Message {
		updated := existing.DeepCopy()
		updated.Message = event.Message
		updated.Severity = severity
		updated.EventDetails = details
		_, err = c.notifications.Update(updated)
		return err
	}

	return nil
}

func (c *controller) runGC(ctx context.Context) {
	for range ticker.Context(ctx, gcInterval) {
		enabled := parseEnabledCategories(settings.UserNotificationEventTypes.Get())
		for _, cat := range enabled {
			c.cleanupAcknowledgedNotifications(cat)
		}
	}
}

func (c *controller) cleanupAcknowledgedNotifications(cat EventCategory) {
	sel, err := labels.Parse(fmt.Sprintf("%s=%s,%s=%s", categoryLabel, cat.Category, clusterNameLabel, c.clusterName))
	if err != nil {
		return
	}

	existing, err := c.notificationCache.List(sel)
	if err != nil {
		return
	}

	for _, n := range existing {
		if n.AcknowledgedAt != nil && time.Since(n.AcknowledgedAt.Time) > acknowledgeMaxAge {
			if err := c.notifications.Delete(n.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				logrus.Errorf("[notification-event-watcher] error deleting acknowledged notification %s: %v", n.Name, err)
			}
		}
	}
}

func notificationName(category, clusterName string, event *corev1.Event) string {
	key := fmt.Sprintf("%s/%s/%s/%s", event.InvolvedObject.Namespace, event.InvolvedObject.Name, event.InvolvedObject.Kind, event.Reason)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))[:8]
	name := fmt.Sprintf("%s-%s-%s", category, clusterName, hash)
	if len(name) > 253 {
		name = name[:253]
	}
	return name
}

func mapSeverity(event *corev1.Event) string {
	if event.Type == corev1.EventTypeWarning {
		return "warning"
	}
	return "info"
}

func buildDetails(event *corev1.Event) map[string]string {
	details := map[string]string{
		"eventReason": event.Reason,
	}
	if event.InvolvedObject.Name != "" {
		details["involvedObjectName"] = event.InvolvedObject.Name
	}
	if event.InvolvedObject.Namespace != "" {
		details["involvedObjectNamespace"] = event.InvolvedObject.Namespace
	}
	if event.InvolvedObject.Kind != "" {
		details["involvedObjectKind"] = event.InvolvedObject.Kind
	}
	if event.Source.Component != "" {
		details["sourceComponent"] = event.Source.Component
	}
	if !event.LastTimestamp.IsZero() {
		details["lastSeen"] = event.LastTimestamp.Format(time.RFC3339)
	}
	if event.Count > 0 {
		details["count"] = fmt.Sprintf("%d", event.Count)
	}
	msg := strings.TrimSpace(event.Message)
	if msg != "" {
		details["eventMessage"] = msg
	}
	return details
}
