package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serviceName = "login-tracer"

func main() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	ctx := context.Background()

	tp, err := initTracer(ctx, endpoint)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("tracer shutdown: %v", err)
		}
	}()

	fmt.Printf("Sending login trace to %s...\n", endpoint)
	simulateLogin(ctx, "user@example.com")
	fmt.Println("Done. Check Datadog APM > Traces for 'login-tracer'.")
}

func initTracer(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
			semconv.DeploymentEnvironment("local"),
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
	return tp, nil
}

var tracer = otel.Tracer(serviceName)

// simulateLogin runs a fake login flow with child spans for each step.
func simulateLogin(ctx context.Context, email string) {
	ctx, span := tracer.Start(ctx, "login",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("user.email", email),
			attribute.String("http.method", "POST"),
			attribute.String("http.route", "/auth/login"),
		),
	)
	defer span.End()

	start := time.Now()
	log.Printf("[login] starting for %s", email)

	if err := validateInput(ctx, email); err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return
	}

	userID, err := lookupUser(ctx, email)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return
	}

	if err := verifyPassword(ctx, userID); err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return
	}

	token, err := generateToken(ctx, userID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return
	}

	span.SetStatus(codes.Ok, "login successful")
	span.SetAttributes(
		attribute.String("user.id", userID),
		attribute.String("session.token_prefix", token[:8]+"..."),
	)

	elapsed := time.Since(start)
	log.Printf("[login] SUCCESS for %s (user_id=%s) in %s", email, userID, elapsed)
}

func validateInput(ctx context.Context, email string) error {
	ctx, span := tracer.Start(ctx, "validate.input",
		trace.WithAttributes(
			attribute.String("user.email", email),
		),
	)
	defer span.End()

	sleep(10, 30)
	span.SetAttributes(attribute.Bool("valid", true))
	span.SetStatus(codes.Ok, "")
	log.Printf("  [validate] email=%s OK", email)
	return nil
}

func lookupUser(ctx context.Context, email string) (string, error) {
	ctx, span := tracer.Start(ctx, "db.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemPostgreSQL,
			semconv.DBOperationKey.String("SELECT"),
			semconv.DBStatementKey.String("SELECT id FROM users WHERE email = ?"),
			attribute.String("db.table", "users"),
		),
	)
	defer span.End()

	sleep(50, 150)
	userID := fmt.Sprintf("usr_%d", rand.Intn(900000)+100000)
	span.SetAttributes(attribute.String("user.id", userID))
	span.SetStatus(codes.Ok, "")
	log.Printf("  [db] found user_id=%s", userID)
	return userID, nil
}

func verifyPassword(ctx context.Context, userID string) error {
	ctx, span := tracer.Start(ctx, "auth.verify_password",
		trace.WithAttributes(
			attribute.String("user.id", userID),
			attribute.String("hash.algorithm", "bcrypt"),
			attribute.Int("hash.cost", 12),
		),
	)
	defer span.End()

	sleep(80, 200)
	span.SetStatus(codes.Ok, "")
	log.Printf("  [auth] password verified for user_id=%s", userID)
	return nil
}

func generateToken(ctx context.Context, userID string) (string, error) {
	_, span := tracer.Start(ctx, "auth.generate_token",
		trace.WithAttributes(
			attribute.String("user.id", userID),
			attribute.Int("token.expiry_hours", 24),
		),
	)
	defer span.End()

	sleep(20, 60)
	token := fmt.Sprintf("%x%x%x", rand.Int63(), rand.Int63(), rand.Int63())
	span.SetStatus(codes.Ok, "")
	log.Printf("  [auth] token generated (prefix=%s...)", token[:8])
	return token, nil
}

func sleep(minMs, maxMs int) {
	d := time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond
	time.Sleep(d)
}
