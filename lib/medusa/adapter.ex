defmodule Medusa.Adapter do
  @moduledoc """
  Behaviour for creating event queue adapters.

  The objective of having this adapter is to be able to switch them if
  you have specific requirements, such as: using Redis, RabbitMQ, AQMP.

  By default Medusa leverages Erlang's `:ets` module to build an in-memory
  database.

  """

  @callback handle_configuration(config :: map) :: map
  @callback insert(type :: String.t, payload :: %Medusa.Event{}) :: {:ok, %Medusa.Event{}} | {:error, String.t}
  @callback insert!(type :: String.t, payload :: %Medusa.Event{}) :: :ok | no_return()
  @callback next(type :: String.t) :: {:ok, %Medusa.Event{}} | {:error, String.t}
  @callback next!(type :: String.t) :: :ok | no_return()
  
end