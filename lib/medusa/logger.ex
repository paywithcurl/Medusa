defmodule Medusa.Logger do
  require Logger

  def info(message, opts \\ [])

  def info(message, opts) when is_binary(message) do
    message |> Poison.decode! |> info(opts)
  end

  def info(%{"body" => body, "metadata" => metadata, "topic" => topic}, opts) do
    info(%{body: body, metadata: metadata, topic: topic}, opts)
  end

  def info(%{body: body, metadata: _, topic: _} = message, opts) do
    message
    |> base_message
    |> Map.merge(%{level: "info", body: body, processing_time: opts[:processing_time]})
    |> Poison.encode!
    |> Logger.info
  end

  def error(%{"body" => body, "metadata" => metadata, "topic" => topic}, reason) do
    error(%{body: body, metadata: metadata, topic: topic}, reason)
  end

  def error(%{body: _, metadata: _, topic: _} = message, reason) do
    message
    |> base_message
    |> Map.merge(%{level: "error", reason: reason})
    |> Poison.encode!
    |> Logger.error
  end

  def error(message, reason) when is_binary(message) do
    message |> Poison.decode! |> error(reason)
  end

  defp base_message(%{metadata: metadata, topic: topic}) do
    message_info = metadata["message_info"] || %Medusa.Message.Info{}
    rabbit_conf = Application.get_env(:medusa, Medusa)[:RabbitMQ][:connection]
    %{
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

  defp base_message(%{"metadata" => metadata, "topic" => topic}) do
    base_message(%{metadata: metadata, topic: topic})
  end
end
