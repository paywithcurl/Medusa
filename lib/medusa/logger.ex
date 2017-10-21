defmodule Medusa.Logger do
  require Logger

  def info(message, opts) when is_binary(message) do
    message
    |> Poison.decode!
    |> info(opts)
  end

  def info(%{"metadata" => metadata, "topic" => topic}, opts) do
    info(%{metadata: metadata, topic: topic}, opts)
  end

  def info(message, opts) do
    belongs = opts |> Keyword.fetch!(:belongs) |> to_string
    message
    |> base_message(belongs)
    |> Map.merge(%{level: "info", processing_time: opts[:processing_time]})
    |> format_log_message
    |> Logger.info
  end

  def debug(message, opts) when is_binary(message) do
    message
    |> Poison.decode!
    |> debug(opts)
  end

  def debug(
      %{"body" => body,
        "metadata" => metadata,
        "topic" => topic},
      opts) do
    debug(%{body: body, metadata: metadata, topic: topic}, opts)
  end

  def debug(
      %{body: body,
        metadata: _,
        topic: _} = message,
      opts) do
    belongs = opts |> Keyword.fetch!(:belongs) |> to_string
    message
    |> base_message_debug(belongs)
    |> Map.merge(%{level: "debug", body: body, processing_time: opts[:processing_time]})
    |> format_log_message
    |> Logger.debug
  end

  def error(message, opts) when is_binary(message) do
    message
    |> Poison.decode!
    |> error(opts)
  end

  def error(
      %{"body" => body,
        "metadata" => metadata,
        "topic" => topic},
      opts) do
    error(%{body: body, metadata: metadata, topic: topic}, opts)
  end

  def error(%{body: _, metadata: _, topic: _} = message, opts) do
    belongs = opts |> Keyword.fetch!(:belongs) |> to_string
    reason = opts |> Keyword.fetch!(:reason)
    message
    |> base_message(belongs)
    |> Map.merge(%{level: "error", reason: reason})
    |> format_log_message
    |> Logger.error
  end

  defp base_message(%{metadata: metadata, topic: topic}, _belongs) do
    message_info = metadata["message_info"] || %Medusa.Message.Info{}
    %{
      topic: topic,
      routing_key: message_info.routing_key,
      message_id: message_info.message_id || metadata["id"],
      request_id: metadata["request_id"],
      origin: metadata["origin"],
    }
  end

  defp base_message(%{"metadata" => metadata, "topic" => topic}, belongs) do
    base_message(%{metadata: metadata, topic: topic}, belongs)
  end

  defp base_message_debug(%{metadata: metadata, topic: topic}, belongs) do
    message_info = metadata["message_info"] || %Medusa.Message.Info{}
    rabbit_conf = Application.get_env(:medusa, Medusa)[:RabbitMQ][:connection]
    %{
      belongs: belongs,
      timestamp: DateTime.utc_now(),
      topic: topic,
      routing_key: message_info.routing_key,
      message_id: message_info.message_id || metadata["id"],
      request_id: metadata["request_id"],
      origin: metadata["origin"],
      rabbitmq_host: rabbit_conf[:host],
      rabbitmq_port: rabbit_conf[:port],
      rabbitmq_exchange: message_info.exchange,
      rabbitmq_consumer_tag: message_info.consumer_tag
    }
  end

  defp base_message_debug(%{"metadata" => metadata, "topic" => topic}, belongs) do
    base_message(%{metadata: metadata, topic: topic}, belongs)
  end

  defp format_log_message(message) do
    case Mix.env() do
      :dev -> inspect(message, pretty: true)
      _ -> Poison.encode!(message)
    end
  end
end
