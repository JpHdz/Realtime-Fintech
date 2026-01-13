use opentelemetry::global;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
pub fn init_tracing(service_name: &str){
  // Declare context propagation in order to get id trace travelling between services
  global::set_text_map_propagator(TraceContextPropagator::new()); 


  let tracer = opentelemetry_otlp::new_pipeline().tracing().with_exporter(opentelemetry_otlp::new_exporter().tonic()).with_trace_config(opentelemetry_sdk::trace::config().with_resource(opentelemetry_sdk::Resource::new(vec![
    opentelemetry::KeyValue::new("service.name", service_name.to_string())
  ]))).install_batch(opentelemetry_sdk::runtime::Tokio).expect("Error while initizing tracer");

  let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

  let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

  tracing_subscriber::registry().with(filter_layer).with(telemetry_layer).with(tracing_subscriber::fmt::layer()).init();

}