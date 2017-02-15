defmodule Medusa.Message do
  defstruct topic: "", body: %{}, metadata: %{}

  defmodule Info do
    defstruct [
      channel: nil,
      delivery_tag: nil,
      routing_key: nil,
      consumer_tag: nil,
      message_id: nil,
      exchange: nil,
      retry: 0
    ]
  end
end
