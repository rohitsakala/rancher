package notification

import (
	"slices"

	"github.com/rancher/norman/types"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"k8s.io/apimachinery/pkg/labels"
)

type Store struct {
	types.Store
	crtbLister v3.ClusterRoleTemplateBindingLister
	grbLister  v3.GlobalRoleBindingLister
	grLister   v3.GlobalRoleLister
}

func NewStore(store types.Store, crtbLister v3.ClusterRoleTemplateBindingLister, grbLister v3.GlobalRoleBindingLister, grLister v3.GlobalRoleLister) *Store {
	return &Store{
		Store:      store,
		crtbLister: crtbLister,
		grbLister:  grbLister,
		grLister:   grLister,
	}
}

func (s *Store) List(apiContext *types.APIContext, schema *types.Schema, opt *types.QueryOptions) ([]map[string]any, error) {
	results, err := s.Store.List(apiContext, schema, opt)
	if err != nil {
		return nil, err
	}

	callerID := apiContext.Request.Header.Get("Impersonate-User")
	if callerID == "" {
		return results, nil
	}

	if s.isAdmin(callerID) {
		return results, nil
	}

	accessibleClusters := s.getClustersForUser(callerID)

	var filtered []map[string]any
	for _, item := range results {
		clusterName, _ := item["clusterName"].(string)
		if clusterName == "" {
			filtered = append(filtered, item)
			continue
		}
		if accessibleClusters[clusterName] {
			filtered = append(filtered, item)
		}
	}

	return filtered, nil
}

func (s *Store) isAdmin(callerID string) bool {
	grbs, err := s.grbLister.List("", labels.Everything())
	if err != nil {
		return false
	}
	for _, grb := range grbs {
		if grb.UserName != callerID {
			continue
		}
		gr, err := s.grLister.Get("", grb.GlobalRoleName)
		if err != nil {
			continue
		}
		for _, rule := range gr.Rules {
			if containsStr(rule.Resources, "*") && containsStr(rule.APIGroups, "*") && containsStr(rule.Verbs, "*") {
				return true
			}
		}
	}
	return false
}

func (s *Store) getClustersForUser(callerID string) map[string]bool {
	clusters := map[string]bool{}
	crtbs, err := s.crtbLister.List("", labels.Everything())
	if err != nil {
		return clusters
	}
	for _, crtb := range crtbs {
		if crtb.UserName == callerID {
			clusters[crtb.ClusterName] = true
		}
	}
	return clusters
}

func containsStr(ss []string, s string) bool {
	return slices.Contains(ss, s)
}
