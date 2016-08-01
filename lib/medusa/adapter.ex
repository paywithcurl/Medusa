defmodule Medusa.Adapter do
  @moduledoc """
  Behaviour for creating event queue adapters.

  The objective of having this adapter is to be able to switch them if
  you have specific requirements, such as: using Redis, RabbitMQ, AQMP.

  By default Medusa leverages Erlang's `:queue` module for each type of
  event.

  """

  @callback insert(type :: String.t, payload :: any) :: :ok | :error
  @callback next(type :: String.t) :: [] | [any]

end