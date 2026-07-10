package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

func getIperfServerClusterIP(clientset *kubernetes.Clientset, ctx context.Context, isUdp bool) (string, string, error) {
	iperfSvc, err := clientset.CoreV1().
		Services("default").
		Get(ctx, "iperf-server", metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	peerIP := iperfSvc.Spec.ClusterIP
	peerPort := iperfSvc.Spec.Ports[0].Port

	return peerIP, fmt.Sprintf("%d", peerPort), nil
}

func getTurnServerIP(clientset *kubernetes.Clientset, ctx context.Context) (string, error) {

	svc, err := clientset.CoreV1().
		Services("stunner").
		Get(ctx, "udp-gateway", metav1.GetOptions{})
	if err != nil {
		panic(err)
	}

	turnIP := svc.Status.LoadBalancer.Ingress[0].IP
	return turnIP, nil
}

func getTurnServerPortFromGateway(dynClient dynamic.Interface, ctx context.Context) (string, error) {

	gvr := schema.GroupVersionResource{
		Group:    "gateway.networking.k8s.io",
		Version:  "v1",
		Resource: "gateways",
	}

	gateway, err := dynClient.Resource(gvr).Namespace("stunner").Get(
		context.TODO(),
		"udp-gateway",
		metav1.GetOptions{},
	)
	if err != nil {
		panic(err)
	}

	turnPort := ""
	// Navigate the unstructured object
	listeners, found, err := unstructured.NestedSlice(gateway.Object, "spec", "listeners")
	if err != nil || !found {
		panic("listeners not found")
	}

	for _, listener := range listeners {
		listener, ok := listener.(map[string]interface{})
		if !ok {
			continue
		}
		if listener["name"] == "udp-listener" {
			// Convert to string
			turnPort = fmt.Sprintf("%d", listener["port"].(int64))
			break
		}
	}

	return turnPort, nil
}
