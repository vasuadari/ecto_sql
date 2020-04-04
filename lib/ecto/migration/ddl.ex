defmodule Ecto.Migration.DDL do
  alias Ecto.Migration.{Constraint, Index, Table}

  def execute(statement) when is_binary(statement) or is_list(statement) do
    "execute #{inspect statement}"
  end

  def execute({:create, %Table{} = table, _}) do
    "create table #{quote_name(table.prefix, table.name)}"
  end

  def execute({:create_if_not_exists, %Table{} = table, _}) do
    "create table if not exists #{quote_name(table.prefix, table.name)}"
  end

  def execute({:alter, %Table{} = table, _}) do
    "alter table #{quote_name(table.prefix, table.name)}"
  end

  def execute({:drop, %Table{} = table}) do
    "drop table #{quote_name(table.prefix, table.name)}"
  end

  def execute({:drop_if_exists, %Table{} = table}) do
    "drop table if exists #{quote_name(table.prefix, table.name)}"
  end

  def execute({:create, %Index{} = index}) do
    "create index #{quote_name(index.prefix, index.name)}"
  end

  def execute({:create_if_not_exists, %Index{} = index}) do
    "create index if not exists #{quote_name(index.prefix, index.name)}"
  end

  def execute({:drop, %Index{} = index}) do
    "drop index #{quote_name(index.prefix, index.name)}"
  end

  def execute({:drop_if_exists, %Index{} = index}) do
    "drop index if exists #{quote_name(index.prefix, index.name)}"
  end

  def execute({:rename, %Table{} = current_table, %Table{} = new_table}) do
    "rename table #{quote_name(current_table.prefix, current_table.name)} to #{quote_name(new_table.prefix, new_table.name)}"
  end

  def execute({:rename, %Table{} = table, current_column, new_column}) do
    "rename column #{current_column} to #{new_column} on table #{quote_name(table.prefix, table.name)}"
  end

  def execute({:create, %Constraint{check: nil, exclude: nil}}) do
    raise ArgumentError, "a constraint must have either a check or exclude option"
  end

  def execute({:create, %Constraint{check: check, exclude: exclude}}) when is_binary(check) and is_binary(exclude) do
    raise ArgumentError, "a constraint must not have both check and exclude options"
  end

  def execute({:create, %Constraint{check: check} = constraint}) when is_binary(check) do
    "create check constraint #{constraint.name} on table #{quote_name(constraint.prefix, constraint.table)}"
  end

  def execute({:create, %Constraint{exclude: exclude} = constraint}) when is_binary(exclude) do
    "create exclude constraint #{constraint.name} on table #{quote_name(constraint.prefix, constraint.table)}"
  end

  def execute({:drop, %Constraint{} = constraint}) do
    "drop constraint #{constraint.name} from table #{quote_name(constraint.prefix, constraint.table)}"
  end

  def execute({:drop_if_exists, %Constraint{} = constraint}) do
    "drop constraint if exists #{constraint.name} from table #{quote_name(constraint.prefix, constraint.table)}"
  end

  defp quote_name(nil, name), do: quote_name(name)
  defp quote_name(prefix, name), do: quote_name(prefix) <> "." <> quote_name(name)
  defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))
  defp quote_name(name), do: name
end
