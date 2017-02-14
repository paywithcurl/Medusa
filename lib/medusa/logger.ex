defmodule Medusa.LogMessage do

  def info(message) when is_map(message) do
    message
    |> base_message
    |> Map.merge(%{level: "info", body: message["body"]})
    |> Poison.encode!
  end

  def info(message) when is_binary(message) do
    message |> Poison.decode! |> info
  end

  def error(reason, message) when is_map(message) do
    message = Poison.encode!(message)
    error(reason, message)
  end

  def error(reason, message) when is_binary(message) do
    message
    |> Poison.decode!
    |> base_message
    |> Map.merge(%{level: "error", reason: reason})
    |> Poison.encode!
  end

  defp base_message(%{metadata: metadata, topic: topic}) do
    rabbit_info = rabbit_info()
    %{
      timestamp: Time.utc_now(),
      routing_key: topic,
      message_id: metadata["id"] || "",
      request_id: metadata["request_id"] || "",
      origin: metadata["origin"] || "",
      rabbitmq_host: rabbit_info[:host],
      rabbitmq_port: rabbit_info[:port],
      rabbitmq_exchange: "",
      rabbitmq_channel: ""
    }
  end

  defp base_message(%{"metadata" => metadata, "topic" => topic}) do
    base_message(%{metadata: metadata, topic: topic})
  end

  defp rabbit_info do
    rabbit_info = Application.get_env(:medusa, Medusa)[:RabbitMQ]
    %{host: rabbit_info[:connection][:host],
      port: rabbit_info[:connection][:port]}
  end
end
