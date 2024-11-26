package otel

/*
import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	prov *metric.MeterProvider
}

func New(prov *metric.MeterProvider) *Metrics {

	meter := otel.Meter("silo")

	//var rollCnt metric.Int64Counter

	rollCnt, err := meter.Int64Gauge("testing", metric.WithDescription("The number of rolls by roll value"))

	if err != nil {
		panic(err)
	}

	return &Metrics{
		prov: prov,
	}
}
*/
