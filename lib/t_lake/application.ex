defmodule TLake.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    gen_launcher = %{
      :id => TLake.GenLauncher,
      :start => {TLake.GenLauncher, :start_link, []},
      :restart => :permanent,
      :shutdown => 5_000,
      :type => :worker
    }

    task_consumer = {TLake.TaskConsumer, Application.fetch_env!(:t_lake, :max_demand)}
    task_producer = {TLake.TaskProducer, []}

    children = [
      # Starts a worker by calling: TLake.Worker.start_link(arg)
      # {TLake.Worker, arg}
      task_producer,
      task_consumer,
      gen_launcher
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :rest_for_one, name: TLake.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
