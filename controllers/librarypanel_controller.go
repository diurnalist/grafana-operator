/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"k8s.io/utils/strings/slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	genapi "github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/client/folders"
	"github.com/grafana/grafana-openapi-client-go/client/library_elements"
	"github.com/grafana/grafana-openapi-client-go/client/search"
	"github.com/grafana/grafana-openapi-client-go/models"
	"github.com/grafana/grafana-operator/v5/api/v1beta1"
	client2 "github.com/grafana/grafana-operator/v5/controllers/client"
	"github.com/grafana/grafana-operator/v5/controllers/content"
	"github.com/grafana/grafana-operator/v5/controllers/metrics"
	kuberr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type LibraryElementType int

const (
	conditionLibraryPanelSynchronized = "LibraryPanelSynchronized"

	LibraryElementTypePanel    LibraryElementType = 1
	LibraryElementTypeVariable LibraryElementType = 2
)

// GrafanaLibraryPanelReconciler reconciles a GrafanaLibraryPanel object
type GrafanaLibraryPanelReconciler struct {
	Client    client.Client
	Scheme    *runtime.Scheme
	Discovery discovery.DiscoveryInterface
}

//+kubebuilder:rbac:groups=grafana.integreatly.org,resources=grafanalibrarypanels,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=grafana.integreatly.org,resources=grafanalibrarypanels/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=grafana.integreatly.org,resources=grafanalibrarypanels/finalizers,verbs=update

func (r *GrafanaLibraryPanelReconciler) syncLibraryPanels(ctx context.Context) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	libraryPanelsSynced := 0

	// get all grafana instances
	grafanas := &v1beta1.GrafanaList{}
	var opts []client.ListOption
	err := r.Client.List(ctx, grafanas, opts...)
	if err != nil {
		return ctrl.Result{
			Requeue: true,
		}, err
	}

	// no instances, no need to sync
	if len(grafanas.Items) == 0 {
		return ctrl.Result{Requeue: false}, nil
	}

	// get all libraryPanels
	allLibraryPanels := &v1beta1.GrafanaLibraryPanelList{}
	err = r.Client.List(ctx, allLibraryPanels, opts...)
	if err != nil {
		return ctrl.Result{
			Requeue: true,
		}, err
	}

	libraryPanelsToDelete := getLibraryPanelsToDelete(allLibraryPanels, grafanas.Items)

	// delete all libraryPanels that no longer have a cr
	for grafana, oldLibraryPanels := range libraryPanelsToDelete {
		grafanaClient, err := client2.NewGeneratedGrafanaClient(ctx, r.Client, grafana)
		if err != nil {
			return ctrl.Result{Requeue: true}, err
		}

		for _, libraryPanel := range oldLibraryPanels {
			// avoid bombarding the grafana instance with a large number of requests at once, limit
			// the sync to a certain number of libraryPanels per cycle. This means that it will take longer to sync
			// a large number of deleted libraryPanel crs, but that should be an edge case.
			if libraryPanelsSynced >= syncBatchSize {
				return ctrl.Result{Requeue: true}, nil
			}

			namespace, name, uid := libraryPanel.Split()

			switch hasConnections, err := libraryElementHasConnections(grafanaClient, uid); {
			case err != nil:
				return ctrl.Result{
					Requeue: true,
				}, err
			case hasConnections:
				log.Info("library panel is connected to other elements, please remove the connections first", "namespace", namespace, "name", name)
				// continue processing other library panels; we don't want this one to block the entire batch.
				continue
			}

			_, err = grafanaClient.LibraryElements.DeleteLibraryElementByUID(uid) //nolint:errcheck
			if err != nil {
				var notFound *library_elements.DeleteLibraryElementByUIDNotFound
				if errors.As(err, &notFound) {
					log.Info("library panel no longer exists", "namespace", namespace, "name", name)
				} else {
					return ctrl.Result{Requeue: false}, err
				}
			}

			grafana.Status.LibraryPanels = grafana.Status.LibraryPanels.Remove(namespace, name)
			libraryPanelsSynced += 1
		}

		// one update per grafana - this will trigger a reconcile of the grafana controller
		// so we should minimize those updates
		err = r.Client.Status().Update(ctx, grafana)
		if err != nil {
			return ctrl.Result{Requeue: false}, err
		}
	}

	if libraryPanelsSynced > 0 {
		log.Info("successfully synced library panels", "libraryPanels", libraryPanelsSynced)
	}
	return ctrl.Result{Requeue: false}, nil
}

// sync libraryPanels, delete libraryPanels from grafana that do no longer have a cr
func getLibraryPanelsToDelete(allLibraryPanels *v1beta1.GrafanaLibraryPanelList, grafanas []v1beta1.Grafana) map[*v1beta1.Grafana][]v1beta1.NamespacedResource {
	libraryPanelsToDelete := map[*v1beta1.Grafana][]v1beta1.NamespacedResource{}
	for _, grafana := range grafanas {
		grafana := grafana
		for _, libraryPanel := range grafana.Status.LibraryPanels {
			if allLibraryPanels.Find(libraryPanel.Namespace(), libraryPanel.Name()) == nil {
				libraryPanelsToDelete[&grafana] = append(libraryPanelsToDelete[&grafana], libraryPanel)
			}
		}
	}
	return libraryPanelsToDelete
}

// libraryElementHasConnections returns whether a library panel is still connected to any dashboards
func libraryElementHasConnections(grafanaClient *genapi.GrafanaHTTPAPI, uid string) (bool, error) {
	resp, err := grafanaClient.LibraryElements.GetLibraryElementConnections(uid)
	if err != nil {
		return false, err
	}
	return len(resp.Payload.Result) > 0, nil
}

func (r *GrafanaLibraryPanelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithName("GrafanaLibraryPanelReconciler")
	ctx = logf.IntoContext(ctx, log)

	// periodic sync reconcile
	if req.Namespace == "" && req.Name == "" {
		start := time.Now()
		syncResult, err := r.syncLibraryPanels(ctx)
		elapsed := time.Since(start).Milliseconds()
		metrics.InitialLibraryPanelSyncDuration.Set(float64(elapsed))
		return syncResult, err
	}

	cr := &v1beta1.GrafanaLibraryPanel{}
	err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Name,
	}, cr)
	if err != nil {
		if kuberr.IsNotFound(err) {
			err = r.onLibraryPanelDeleted(ctx, req.Namespace, req.Name)
			if err != nil {
				return ctrl.Result{RequeueAfter: RequeueDelay}, err
			}
			return ctrl.Result{}, nil
		}
		log.Error(err, "error getting grafana library panel cr")
		return ctrl.Result{RequeueAfter: RequeueDelay}, err
	}

	instances, err := r.GetMatchingLibraryPanelInstances(ctx, cr, r.Client)
	if err != nil {
		log.Error(err, "could not find matching instances", "name", cr.Name, "namespace", cr.Namespace)
		return ctrl.Result{RequeueAfter: RequeueDelay}, err
	}

	removeNoMatchingInstance(&cr.Status.Conditions)
	log.Info("found matching Grafana instances for library panel", "count", len(instances.Items))

	resolver, err := content.NewContentResolver(cr, r.Client)
	if err != nil {
		log.Error(err, "error creating library panel content resolver", "libraryPanel", cr.Name)
		// Failing to create a resolver is an unrecoverable error
		return ctrl.Result{Requeue: false}, nil
	}

	// Retrieving the model before the loop ensures to exit early in case of failure and not fail once per matching instance
	libraryPanelModel, hash, err := resolver.Resolve(ctx)
	if err != nil {
		log.Error(err, "error resolving library panel contents", "libraryPanel", cr.Name)
		return ctrl.Result{RequeueAfter: RequeueDelay}, nil
	}

	uid := fmt.Sprintf("%s", libraryPanelModel["uid"])

	// Garbage collection for a case where uid get changed, creation is expected to happen in a separate reconcilication cycle
	if content.IsUpdatedUID(cr, uid) {
		log.Info("library panel uid got updated, deleting library panel with the old uid")
		err = r.onLibraryPanelDeleted(ctx, req.Namespace, req.Name)
		if err != nil {
			return ctrl.Result{RequeueAfter: RequeueDelay}, err
		}

		// Clean up uid, so further reconciliations can track changes there
		cr.Status.UID = ""
		err = r.Client.Status().Update(ctx, cr)
		if err != nil {
			return ctrl.Result{RequeueAfter: RequeueDelay}, err
		}

		// Status update should trigger the next reconciliation right away, no need to requeue for creation
		return ctrl.Result{}, nil
	}

	success := true
	applyErrors := make(map[string]string)
	for _, grafana := range instances.Items {
		// check if this is a cross namespace import
		if grafana.Namespace != cr.Namespace && !cr.IsAllowCrossNamespaceImport() {
			continue
		}

		grafana := grafana
		// an admin url is required to interact with grafana
		// the instance or route might not yet be ready
		if grafana.Status.Stage != v1beta1.OperatorStageComplete || grafana.Status.StageStatus != v1beta1.OperatorStageResultSuccess {
			log.Info("grafana instance not ready", "grafana", grafana.Name)
			success = false
			continue
		}

		if grafana.IsInternal() {
			// first reconcile the plugins
			// append the requested library panels to a configmap from where the
			// grafana reconciler will pick them up
			err = ReconcilePlugins(ctx, r.Client, r.Scheme, &grafana, cr.Spec.Plugins, fmt.Sprintf("%v-librarypanel", cr.Name))
			if err != nil {
				log.Error(err, "error reconciling plugins", "libraryPanel", cr.Name, "grafana", grafana.Name)
				success = false
			}
		}

		// then import the libraryPanel into the matching grafana instances
		err = r.onLibraryPanelCreated(ctx, &grafana, cr, libraryPanelModel, hash)
		if err != nil {
			log.Error(err, "error reconciling library panel", "libraryPanel", cr.Name, "grafana", grafana.Name)
			applyErrors[fmt.Sprintf("%s/%s", grafana.Namespace, grafana.Name)] = err.Error()
			success = false
		}

		condition := buildSynchronizedCondition("LibraryPanel", conditionLibraryPanelSynchronized, cr.Generation, applyErrors, len(instances.Items))
		meta.SetStatusCondition(&cr.Status.Conditions, condition)
	}

	// if successfully synced in all instances, wait for its re-sync period
	if success {
		if cr.ResyncPeriodHasElapsed() {
			cr.Status.LastResync = metav1.Time{Time: time.Now()}
		}
		cr.Status.Hash = hash
		cr.Status.UID = uid
		return ctrl.Result{RequeueAfter: cr.Spec.ResyncPeriod.Duration}, r.Client.Status().Update(ctx, cr)
	}

	return ctrl.Result{RequeueAfter: RequeueDelay}, nil
}

func (r *GrafanaLibraryPanelReconciler) onLibraryPanelDeleted(ctx context.Context, namespace string, name string) error {
	log := logf.FromContext(ctx)
	list := v1beta1.GrafanaList{}
	var opts []client.ListOption
	err := r.Client.List(ctx, &list, opts...)
	if err != nil {
		return err
	}

	for _, grafana := range list.Items {
		if found, uid := grafana.Status.LibraryPanels.Find(namespace, name); found {
			grafana := grafana
			grafanaClient, err := client2.NewGeneratedGrafanaClient(ctx, r.Client, &grafana)
			if err != nil {
				return err
			}

			isCleanupInGrafanaRequired := true

			resp, err := grafanaClient.LibraryElements.GetLibraryElementByUID(*uid)
			if err != nil {
				var notFound *library_elements.GetLibraryElementByUIDNotFound
				if !errors.As(err, &notFound) {
					return err
				}

				isCleanupInGrafanaRequired = false
			}

			if isCleanupInGrafanaRequired {
				switch hasConnections, err := libraryElementHasConnections(grafanaClient, *uid); {
				case err != nil:
					return err
				case hasConnections:
					msg := "library panel is connected to other elements, please remove the connections first"
					log.Error(err, msg, "action_needed", true, "uid", uid)
					return fmt.Errorf(msg) //nolint
				}

				var wrappedRes *models.LibraryElementResponse
				if resp != nil {
					wrappedRes = resp.GetPayload()
				}

				var elem *models.LibraryElementDTO
				if wrappedRes != nil {
					elem = wrappedRes.Result
				}

				_, err = grafanaClient.LibraryElements.DeleteLibraryElementByUID(*uid) //nolint:errcheck
				if err != nil {
					var notFound *library_elements.DeleteLibraryElementByUIDNotFound
					if !errors.As(err, &notFound) {
						return err
					}
				}

				if elem != nil && elem.Meta != nil && elem.Meta.FolderUID != "" {
					resp, err := r.DeleteFolderIfEmpty(grafanaClient, elem.Meta.FolderUID)
					if err != nil {
						return err
					}
					if resp.StatusCode == 200 {
						log.Info("unused folder successfully removed")
					}
					if resp.StatusCode == 432 {
						log.Info("folder still in use by other resources")
					}
				}
			}

			if grafana.IsInternal() {
				err = ReconcilePlugins(ctx, r.Client, r.Scheme, &grafana, nil, fmt.Sprintf("%v-librarypanel", name))
				if err != nil {
					return err
				}
			}

			grafana.Status.LibraryPanels = grafana.Status.LibraryPanels.Remove(namespace, name)
			err = r.Client.Status().Update(ctx, &grafana)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *GrafanaLibraryPanelReconciler) onLibraryPanelCreated(ctx context.Context, grafana *v1beta1.Grafana, cr *v1beta1.GrafanaLibraryPanel, libraryPanelModel map[string]interface{}, hash string) error {
	log := logf.FromContext(ctx)
	if grafana.IsExternal() && cr.Spec.Plugins != nil {
		return fmt.Errorf("external grafana instances don't support plugins, please remove spec.plugins from your library panel cr")
	}

	grafanaClient, err := client2.NewGeneratedGrafanaClient(ctx, r.Client, grafana)
	if err != nil {
		return err
	}

	folderUID, err := getFolderUID(ctx, r.Client, cr)
	if err != nil {
		return err
	}

	if folderUID == "" {
		folderUID, err = r.GetOrCreateFolder(grafanaClient, cr)
		if err != nil {
			return err
		}
	}

	uid := fmt.Sprintf("%s", libraryPanelModel["uid"])
	name := fmt.Sprintf("%s", libraryPanelModel["name"])

	exists, remoteUID, err := r.Exists(grafanaClient, uid, name, folderUID)
	if err != nil {
		return err
	}

	if exists && remoteUID != uid {
		log.Info("found library panel with the same title (in the same folder) but different uid, removing the library panel before recreating it with a new uid")
		_, err = grafanaClient.LibraryElements.DeleteLibraryElementByUID(remoteUID) //nolint:errcheck
		if err != nil {
			var notFound *library_elements.DeleteLibraryElementByUIDNotFound
			if !errors.As(err, &notFound) {
				return err
			}
		}

		exists = false
	}

	if exists && content.Unchanged(cr, hash) && !cr.ResyncPeriodHasElapsed() {
		return nil
	}

	remoteChanged, err := r.hasRemoteChange(exists, grafanaClient, uid, libraryPanelModel)
	if err != nil {
		return err
	}

	if !remoteChanged {
		return nil
	}

	// nolint:errcheck
	_, err = grafanaClient.LibraryElements.CreateLibraryElement(&models.CreateLibraryElementCommand{
		Model:     libraryPanelModel,
		FolderUID: folderUID,
		Kind:      int64(LibraryElementTypePanel),
	})
	if err != nil {
		return err
	}

	grafana.Status.LibraryPanels = grafana.Status.LibraryPanels.Add(cr.Namespace, cr.Name, uid)
	return r.Client.Status().Update(ctx, grafana)
}

func (r *GrafanaLibraryPanelReconciler) Exists(client *genapi.GrafanaHTTPAPI, uid string, name string, folderUID string) (bool, string, error) {
	resp, err := client.LibraryElements.GetLibraryElementByName(name)

	var panelNotFound *library_elements.GetLibraryElementByNameNotFound
	switch {
	case err != nil:
		return false, "", err
	case errors.As(err, &panelNotFound):
		return false, "", nil
	}

	for _, element := range resp.Payload.Result {
		if element.UID == uid || (element.Name == name && element.FolderUID == folderUID) {
			return true, element.UID, nil
		}
	}

	return false, "", nil
}

// HasRemoteChange checks if a libraryPanel in Grafana is different to the model defined in the custom resources
func (r *GrafanaLibraryPanelReconciler) hasRemoteChange(exists bool, client *genapi.GrafanaHTTPAPI, uid string, model map[string]interface{}) (bool, error) {
	if !exists {
		// if the libraryPanel doesn't exist, don't even request
		return true, nil
	}

	remoteLibraryPanel, err := client.LibraryElements.GetLibraryElementByUID(uid)
	if err != nil {
		var notFound *library_elements.GetLibraryElementByUIDNotFound
		if !errors.As(err, &notFound) {
			return true, nil
		}
		return false, err
	}

	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}

	var payload *models.LibraryElementResponse
	if remoteLibraryPanel != nil {
		payload = remoteLibraryPanel.GetPayload()
	}

	var elem *models.LibraryElementDTO
	if payload != nil {
		elem = payload.Result
	}

	if elem == nil {
		return false, fmt.Errorf("remote library panel is undefined")
	}

	remoteModel, ok := (elem.Model).(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("remote library panel is not an object")
	}

	skipKeys := []string{"id", "version"} //nolint
	for _, key := range keys {
		// we do not keep track of those keys in the custom resource
		if slices.Contains(skipKeys, key) {
			continue
		}
		localValue := model[key]
		remoteValue := remoteModel[key]
		if !reflect.DeepEqual(localValue, remoteValue) {
			return true, nil
		}
	}

	return false, nil
}

func (r *GrafanaLibraryPanelReconciler) GetOrCreateFolder(client *genapi.GrafanaHTTPAPI, cr *v1beta1.GrafanaLibraryPanel) (string, error) {
	title := cr.Namespace
	if cr.Spec.FolderTitle != "" {
		title = cr.Spec.FolderTitle
	}

	exists, folderUID, err := r.GetFolderUID(client, title)
	if err != nil {
		return "", err
	}

	if exists {
		return folderUID, nil
	}

	// Folder wasn't found, let's create it
	body := &models.CreateFolderCommand{
		Title: title,
	}
	resp, err := client.Folders.CreateFolder(body)
	if err != nil {
		return "", err
	}
	folder := resp.GetPayload()
	if folder == nil {
		return "", fmt.Errorf("invalid payload returned")
	}

	return folder.UID, nil
}

func (r *GrafanaLibraryPanelReconciler) GetFolderUID(
	client *genapi.GrafanaHTTPAPI,
	title string,
) (bool, string, error) {
	// Pre-existing folder that is not returned in Folder API
	if strings.EqualFold(title, "General") {
		return true, "", nil
	}
	page := int64(1)
	limit := int64(1000)
	for {
		params := folders.NewGetFoldersParams().WithPage(&page).WithLimit(&limit)

		foldersResp, err := client.Folders.GetFolders(params)
		if err != nil {
			return false, "", err
		}
		folders := foldersResp.GetPayload()

		for _, remoteFolder := range folders {
			if strings.EqualFold(remoteFolder.Title, title) {
				return true, remoteFolder.UID, nil
			}
		}
		if len(folders) < int(limit) {
			break
		}
		page++
	}

	return false, "", nil
}

func (r *GrafanaLibraryPanelReconciler) DeleteFolderIfEmpty(client *genapi.GrafanaHTTPAPI, folderUID string) (http.Response, error) {
	params := search.NewSearchParams().WithFolderUIDs([]string{folderUID})
	results, err := client.Search.Search(params)
	if err != nil {
		return http.Response{
			Status:     "internal grafana client error getting library panels",
			StatusCode: 500,
		}, err
	}
	if len(results.GetPayload()) > 0 {
		return http.Response{
			Status:     "resource is still in use",
			StatusCode: http.StatusLocked,
		}, err
	}

	deleteParams := folders.NewDeleteFolderParams().WithFolderUID(folderUID)
	if _, err = client.Folders.DeleteFolder(deleteParams); err != nil { //nolint:errcheck
		var notFound *folders.DeleteFolderNotFound
		if !errors.As(err, &notFound) {
			return http.Response{
				Status:     "internal grafana client error deleting grafana folder",
				StatusCode: 500,
			}, err
		}
	}
	return http.Response{
		Status:     "grafana folder deleted",
		StatusCode: 200,
	}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrafanaLibraryPanelReconciler) SetupWithManager(mgr ctrl.Manager, ctx context.Context) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.GrafanaLibraryPanel{}).
		Complete(r)

	if err == nil {
		go func() {
			log := logf.FromContext(ctx).WithName("GrafanaLibraryPanelReconciler")
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(initialSyncDelay):
					result, err := r.Reconcile(ctx, ctrl.Request{})
					if err != nil {
						log.Error(err, "error synchronizing library panels")
						continue
					}
					if result.Requeue {
						log.Info("more library panels left to synchronize")
						continue
					}
					log.Info("library panel sync complete")
					return
				}
			}
		}()
	}

	return err
}

func (r *GrafanaLibraryPanelReconciler) GetMatchingLibraryPanelInstances(ctx context.Context, libraryPanel *v1beta1.GrafanaLibraryPanel, k8sClient client.Client) (v1beta1.GrafanaList, error) {
	log := logf.FromContext(ctx)
	instances, err := GetMatchingInstances(ctx, k8sClient, libraryPanel.Spec.InstanceSelector)
	if err != nil || len(instances.Items) == 0 {
		libraryPanel.Status.NoMatchingInstances = true
		if err := r.Client.Status().Update(ctx, libraryPanel); err != nil {
			log.Info("unable to update the status of %v, in %v", libraryPanel.Name, libraryPanel.Namespace)
		}
		return v1beta1.GrafanaList{}, err
	}
	libraryPanel.Status.NoMatchingInstances = false
	if err := r.Client.Status().Update(ctx, libraryPanel); err != nil {
		log.Info("unable to update the status of %v, in %v", libraryPanel.Name, libraryPanel.Namespace)
	}

	return instances, err
}
