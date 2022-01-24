import graphene

from .util import non_null_list


class GrapheneTableConstraints(graphene.ObjectType):
    other = non_null_list(graphene.String)

    class Meta:
        name = "TableConstraints"


class GrapheneTableFieldConstraints(graphene.ObjectType):
    required = graphene.NonNull(graphene.Boolean)
    unique = graphene.NonNull(graphene.Boolean)
    minimum = graphene.String
    maximum = graphene.String
    minLength = graphene.Int
    maxLength = graphene.Int
    pattern = graphene.String
    enum = graphene.List(graphene.NonNull(graphene.String))
    other = non_null_list(graphene.String)

    class Meta:
        name = "TableFieldConstraints"


class GrapheneTableField(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    type = graphene.NonNull(graphene.String)
    description = graphene.String()
    constraints = graphene.NonNull(GrapheneTableFieldConstraints)

    class Meta:
        name = "TableField"


class GrapheneTableSchema(graphene.ObjectType):
    constraints = graphene.Field(GrapheneTableConstraints)
    fields = non_null_list(GrapheneTableField)

    class Meta:
        name = "TableSchema"


class GrapheneTable(graphene.ObjectType):
    schema = graphene.NonNull(GrapheneTableSchema)
    records = non_null_list(graphene.String)  # each element is one record serialized as JSON

    class Meta:
        name = "Table"


types = [
    GrapheneTable,
    GrapheneTableSchema,
    GrapheneTableConstraints,
    GrapheneTableField,
    GrapheneTableFieldConstraints,
]
