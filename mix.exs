defmodule TLake.MixProject do
  use Mix.Project

  def project do
    [
      app: :t_lake,
      version: "1.0.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases(),
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {TLake.Application, []}
    ]
  end

  defp aliases do
    [
      lint: [
        "format",
        "dialyzer",
        "credo"
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # No runtime deps
      {:dialyxir, ">= 1.4.3", only: [:dev], runtime: false},
      {:credo, ">= 1.7.6", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.32.2", only: :dev, runtime: false},

      # Runtime deps
      {:travianmap, "1.0.0"},
      {:explorer, "0.8.2", system_env: %{"EXPLORER_BUILD" => "1"}},
      {:rustler, ">= 0.0.0"}
    ]
  end

  defp releases() do
    [base: [
      include_executables_for: [:unix]
    ]]
  end
end
