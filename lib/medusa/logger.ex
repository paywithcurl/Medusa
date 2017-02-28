defmodule Medusa.Logger do
  require Logger

  defmacro __using__(type) when is_binary(type) or is_atom(type) do
    type = to_string(type)
    quote do
      def logger_belongs, do: unquote(type)
    end
  end

  defmacro info(message, opts \\ []) do
    quote do
      belongs = __MODULE__.logger_belongs()
      Medusa.Logger.info(unquote(message), unquote(opts), belongs)
    end
  end

  defmacro error(message, reason) do
    quote do
      belongs = __MODULE__.logger_belongs()
      Medusa.Logger.error(unquote(message), unquote(reason), belongs)
    end
  end

  def info(message, opts, belongs) when is_binary(message) do
    message |> Poison.decode! |> info(opts, belongs)
  end

  def info(%{"body" => body,
                "metadata" => metadata,
                "topic" => topic},
              opts, belongs) do
    info(%{body: body, metadata: metadata, topic: topic}, opts, belongs)
  end

  def info(%{body: body, metadata: _, topic: _} = message, opts, belongs) do
    message
    |> base_message(belongs)
    |> Map.merge(%{level: "info", body: body, processing_time: opts[:processing_time]})
    |> format_log_message
    |> Logger.info
  end

  def error(%{"body" => body,
                 "metadata" => metadata,
                 "topic" => topic},
                reason, belongs) do
    error(%{body: body, metadata: metadata, topic: topic}, reason, belongs)
  end

  def error(%{body: _, metadata: _, topic: _} = message, reason, belongs) do
    message
    |> base_message(belongs)
    |> Map.merge(%{level: "error", reason: reason})
    |> format_log_message
    |> Logger.error
  end

  def error(message, reason, belongs) when is_binary(message) do
    message |> Poison.decode! |> error(reason, belongs)
  end

  defp base_message(%{metadata: metadata, topic: topic}, belongs) do
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

  defp base_message(%{"metadata" => metadata, "topic" => topic}, belongs) do
    base_message(%{metadata: metadata, topic: topic}, belongs)
  end

  defp format_log_message(message) do
    case Mix.env() do
      :dev -> inspect(message, pretty: true)
      _ -> Poison.encode!(message)
    end
  end
end
