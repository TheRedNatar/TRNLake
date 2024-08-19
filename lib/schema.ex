defmodule TLake.Schema do
  def snapshot() do
    [
      {"server_id", :string},
      {"target_date", :date},
      {"alliance_id", :u32},
      {"alliance_name", :string},
      {"grid_position", :u32},
      {"has_harbor", :boolean},
      {"is_capital", :boolean},
      {"is_city", :boolean},
      {"player_id", :u32},
      {"player_name", :string},
      {"population", :u16},
      {"region", :string},
      {"tribe", :u8},
      {"victory_points", :u32},
      {"village_id", :u32},
      {"village_name", :string},
      {"x_position", :s32},
      {"y_position", :s32},
      {"artifacts_date", :date},
      {"building_plans_date", :date},
      {"end_date", :string},
      {"name", :string},
      {"number_of_tribes", {:u, 8}},
      {"speed", {:u, 8}},
      {"start_date", :date},
      {"timezone", :string},
      {"timezone_offset", {:s, 8}},
      {"url", :string},
      {"version", :string}
    ]
  end
end
