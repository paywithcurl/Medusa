defmodule Medusa.Adapter do
  @moduledoc """
  Behaviour for broker to communicate with Queue

  The objective of having this adapter is to be able to switch them if
  you have specific requirements, such as: using Redis, RabbitMQ, AQMP.
  """

  @callback insert(type :: String.t, payload :: any) :: :ok | :error
  @callback next(type :: String.t) :: [] | [any]

end
