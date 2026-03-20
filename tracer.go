package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "sentry-slo-probe"

var tracer trace.Tracer

// initTracer configures an OTLP/HTTP exporter pointing at Datadog's agentless
// intake and sets it as the global OTel TracerProvider.
func initTracer(ctx context.Context, ddAPIKey, ddSite string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(fmt.Sprintf("api.%s", ddSite)),
		otlptracehttp.WithURLPath("/api/intake/otlp/v1/traces"),
		otlptracehttp.WithHeaders(map[string]string{"DD-API-KEY": ddAPIKey}),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(tracerName),
			semconv.ServiceVersion("1.0.0"),
			semconv.DeploymentEnvironment("probe"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer(tracerName)
	return tp, nil
}
