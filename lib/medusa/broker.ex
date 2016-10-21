defmodule Medusa.Broker do
  @moduledoc false

  defmodule Message do
    defstruct body: %{}, metadata: %{}
  end

  @doc """
  Adds a new route to the broker. If there is an existing route,
  it just ignores it.
  """
  def new_route(event) do
    adapter.new_route(event)
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.
  """
  def publish(event, payload, metadata \\ %{}) do
    message = %Message{body: payload, metadata: metadata}
    adapter.publish(event, message)
  end

  defp adapter do
    :medusa |> Application.get_env(Medusa) |> Keyword.fetch!(:adapter)
  end

end
