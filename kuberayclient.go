package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubeRayClient manages RayJob CRDs via Kubernetes API instead of calling Ray directly.
type KubeRayClient struct {
	k8sClient     client.Client
	clientset     *kubernetes.Clientset
	namespace     string
	clusterName   string // Target persistent RayCluster name
	queueName     string // Kueue queue name
}

// NewKubeRayClient creates a Kubernetes client for managing RayJob CRDs.
func NewKubeRayClient(namespace, clusterName, queueName string) (*KubeRayClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	// Create controller-runtime client for CRDs
	scheme := runtime.NewScheme()
	if err := rayv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add ray scheme: %w", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add core scheme: %w", err)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create clientset for pod logs
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return &KubeRayClient{
		k8sClient:   k8sClient,
		clientset:   clientset,
		namespace:   namespace,
		clusterName: clusterName,
		queueName:   queueName,
	}, nil
}

// SubmitJob creates a RayJob CRD with Kueue integration.
func (c *KubeRayClient) SubmitJob(req SubmitJobRequest) (string, error) {
	ctx := context.Background()

	// Generate unique job name
	jobName := fmt.Sprintf("batch-%d", time.Now().UnixNano())

	// Map priority to PriorityClassName
	priorityClass := ""
	switch req.Metadata["priority"] {
	case "high":
		priorityClass = "high-priority"
	case "medium":
		priorityClass = "medium-priority"
	case "low":
		priorityClass = "low-priority"
	default:
		priorityClass = "medium-priority"
	}

	// Build runtime_env as YAML string
	runtimeEnvYAML, err := buildRuntimeEnvYAML(req.RuntimeEnv)
	if err != nil {
		return "", fmt.Errorf("failed to build runtime_env: %w", err)
	}

	// Sanitize model name for use in labels (replace / with -)
	modelLabel := strings.ReplaceAll(req.Metadata["model"], "/", "-")

	// Create RayJob CRD
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: c.namespace,
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": c.queueName,
				"app":                       "gpu-api",
				"model":                     modelLabel,
			},
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint: req.Entrypoint,
			RuntimeEnvYAML: runtimeEnvYAML,
			ClusterSelector: map[string]string{
				"ray.io/cluster": c.clusterName,
			},
			ShutdownAfterJobFinishes: true, // Required for Kueue suspend/resume
			TTLSecondsAfterFinished: 3600, // Keep submitter pod for 1 hour for log retrieval
			SubmitterPodTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "ray-job-submitter",
							Image: "rayproject/ray:2.53.0-py310",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
					PriorityClassName: priorityClass,
				},
			},
		},
	}

	if err := c.k8sClient.Create(ctx, rayJob); err != nil {
		return "", fmt.Errorf("failed to create RayJob: %w", err)
	}

	return jobName, nil
}

// GetJobStatus retrieves the status of a RayJob CRD.
func (c *KubeRayClient) GetJobStatus(jobID string) (*JobStatusResponse, error) {
	ctx := context.Background()

	var rayJob rayv1.RayJob
	if err := c.k8sClient.Get(ctx, types.NamespacedName{
		Name:      jobID,
		Namespace: c.namespace,
	}, &rayJob); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get RayJob: %w", err)
	}

	// Map RayJob status to Ray Jobs API status
	status := &JobStatusResponse{
		JobID: jobID,
		Metadata: map[string]string{
			"model":    rayJob.Labels["model"],
			"priority": getPriorityFromClass(rayJob.Spec.SubmitterPodTemplate.Spec.PriorityClassName),
		},
	}

	// Check if job was admitted by Kueue
	if rayJob.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusSuspended {
		status.Status = "PENDING"
		status.Message = "Waiting for Kueue admission (resources not available)"
		return status, nil
	}

	// Map RayJob status to API status
	switch rayJob.Status.JobStatus {
	case rayv1.JobStatusNew, rayv1.JobStatusPending:
		status.Status = "PENDING"
	case rayv1.JobStatusRunning:
		status.Status = "RUNNING"
	case rayv1.JobStatusSucceeded:
		status.Status = "SUCCEEDED"
		if rayJob.Status.StartTime != nil && rayJob.Status.EndTime != nil {
			status.StartTime = rayJob.Status.StartTime.Unix() * 1000
			status.EndTime = rayJob.Status.EndTime.Unix() * 1000
		}
	case rayv1.JobStatusFailed:
		status.Status = "FAILED"
		status.Message = rayJob.Status.Message
	case rayv1.JobStatusStopped:
		status.Status = "STOPPED"
	default:
		status.Status = "PENDING"
	}

	return status, nil
}

// GetJobLogs retrieves logs from the RayJob's submitter pod.
func (c *KubeRayClient) GetJobLogs(jobID string) (string, error) {
	ctx := context.Background()

	// Find the submitter pod for this RayJob
	podList := &corev1.PodList{}
	if err := c.k8sClient.List(ctx, podList, client.InNamespace(c.namespace), client.MatchingLabels{
		"ray.io/job-name": jobID,
		"ray.io/node-type": "job",
	}); err != nil {
		return "", fmt.Errorf("failed to list pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no submitter pod found for job %s", jobID)
	}

	pod := podList.Items[0]
	req := c.clientset.CoreV1().Pods(c.namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container: "ray-job-submitter",
	})

	logs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to stream logs: %w", err)
	}
	defer logs.Close()

	var buf strings.Builder
	if _, err := io.Copy(&buf, logs); err != nil {
		return "", fmt.Errorf("failed to read logs: %w", err)
	}

	return buf.String(), nil
}

// Healthz checks if the Kubernetes API is reachable.
func (c *KubeRayClient) Healthz() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to list RayJobs as a health check
	var rayJobList rayv1.RayJobList
	if err := c.k8sClient.List(ctx, &rayJobList, client.InNamespace(c.namespace), client.Limit(1)); err != nil {
		return fmt.Errorf("kubernetes API unreachable: %w", err)
	}
	return nil
}

// --- Helpers ---

func buildRuntimeEnvYAML(env map[string]any) (string, error) {
	if env == nil || len(env) == 0 {
		return "", nil
	}

	var parts []string

	// Handle pip packages
	if pip, ok := env["pip"].([]string); ok && len(pip) > 0 {
		parts = append(parts, "pip:")
		for _, pkg := range pip {
			parts = append(parts, fmt.Sprintf("  - %s", pkg))
		}
	}

	// Handle env_vars
	if envVars, ok := env["env_vars"].(map[string]string); ok && len(envVars) > 0 {
		parts = append(parts, "env_vars:")
		for k, v := range envVars {
			parts = append(parts, fmt.Sprintf("  %s: %q", k, v))
		}
	}

	return strings.Join(parts, "\n"), nil
}

func getPriorityFromClass(className string) string {
	switch className {
	case "high-priority":
		return "high"
	case "medium-priority":
		return "medium"
	case "low-priority":
		return "low"
	default:
		return "medium"
	}
}
