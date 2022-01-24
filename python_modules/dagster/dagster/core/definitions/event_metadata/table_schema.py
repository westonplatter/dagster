from typing import List, NamedTuple, Optional, cast

import dagster.check as check
from dagster.serdes.serdes import DefaultNamedTupleSerializer, whitelist_for_serdes

# ########################
# ##### TABLE SCHEMA
# ########################


class _TableSchemaSerializer(DefaultNamedTupleSerializer):
    pass


@whitelist_for_serdes(serializer=_TableSchemaSerializer)
class TableSchema(
    NamedTuple(
        "TableSchema",
        [
            ("fields", List["TableField"]),
            ("constraints", "TableConstraints"),
        ],
    )
):
    """Representation of a schema for tabular data. Schema format is based on
    `Frictionless Table Schema<https://specs.frictionlessdata.io//table-schema/>`,
    with the following modifications:

    - A top-level property `constraints` MAY be included. This value is a
      descriptor for "table-level" constraints. Presently only one property,
      `other` is supported. This should contain a list of strings describing
      arbitrary table-level constraints.
    - No top-level properties other than `fields` and `constraints` are
      allowed.
    - Field descriptors only support `name`, `type`, `description`, and `constraints` properties.
      `format`, `title`, or arbitrary properties are not allowed.
    - The `type` of a field descriptor is an arbitrary string (i.e. it is not restricted
      to Frictionless types).
    - Field `constraints` descriptor MAY contain a property `other` which
      contains an array of strings. Each element should describe a constraint
      that is not expressible with predefined Frictionless constraint types.

    The schema is constructed out of :py:class:`~dagster.TableConstraints` and
    :py:class:`~dagster.TableField` objects. Example:

    .. code-block:: python

            # example schema
            TableSchema(
                constraints = TableConstraints(
                    "other": [
                        "foo > bar",
                    ],
                ),
                fields = [
                    TableField(
                        name = "foo",
                        type = "string",
                        description = "Foo description",
                        constraints = TableFieldConstraints(
                            required = True,
                            other = [
                                "starts with the letter 'a'",
                            ],
                        ),
                    ),
                    TableField(
                        name = "bar",
                        type = "string",
                    ),
                    TableField(
                        name = "baz",
                        type = "custom_type",
                        constraints = TableFieldConstraints(
                            unique = True,
                        )
                    ),
                ],
            )

    Args:
        fields (List[TableField]): The fields of the table.
        constraints (Optional[TableConstraints]): The constraints of the table.
    """

    def __new__(
        cls,
        fields: List["TableField"],
        constraints: Optional["TableConstraints"] = None,
    ):
        return super(TableSchema, cls).__new__(
            cls,
            fields=check.list_param(fields, "fields", of_type=TableField),
            constraints=check.opt_inst_param(
                constraints, "constraints", TableConstraints, default=_DefaultTableConstraints
            ),
        )


# ########################
# ##### TABLE CONSTRAINTS
# ########################


class _TableConstraintsSerializer(DefaultNamedTupleSerializer):
    pass


@whitelist_for_serdes(serializer=_TableConstraintsSerializer)
class TableConstraints(
    NamedTuple(
        "TableConstraints",
        [
            ("other", List[str]),
        ],
    )
):
    """Descriptor for "table-level" constraints. Presently only one property,
    `other` is supported. This contains strings describing arbitrary
    table-level constraints. A table-level constraint is a constraint defined
    in terms of multiple fields (e.g. col_A > col_B) or in terms of rows.

    Args:
        other (List[str]): Descriptions of arbitrary table-level constraints.
    """

    def __new__(
        cls,
        other: List[str],
    ):
        return super(TableConstraints, cls).__new__(
            cls,
            other=check.list_param(other, "other", of_type=str),
        )


_DefaultTableConstraints = TableConstraints(other=[])

# ########################
# ##### TABLE FIELD
# ########################


class _TableFieldSerializer(DefaultNamedTupleSerializer):
    pass


@whitelist_for_serdes(serializer=_TableFieldSerializer)
class TableField(
    NamedTuple(
        "TableField",
        [
            ("name", str),
            ("type", str),
            ("description", Optional[str]),
            ("constraints", "TableFieldConstraints"),
        ],
    )
):
    """Descriptor for a table field. The only property that must be specified
    by the user is `name`. If no `type` is specified, `string` is assumed. If
    no `constraints` are specified, the field is assumed to be nullable (i.e. `required = False`)
    and have no other constraints beyond the data type.

    Args:
        name (List[str]): Descriptions of arbitrary table-level constraints.
        type (Optional[str]): The type of the field. Can be an arbitrary
            string. Defaults to `"string"`.
        description (Optional[str]): Description of this field. Defaults to `None`.
        constraints (Optional[TableFieldConstraints]): Field-level constraints.
            If unspecified, field is nullable with no constraints.
    """

    def __new__(
        cls,
        name: str,
        type: str = "string",
        description: Optional[str] = None,
        constraints: Optional["TableFieldConstraints"] = None,
    ):
        return super(TableField, cls).__new__(
            cls,
            name=check.str_param(name, "name"),
            type=check.str_param(type, "type"),
            description=check.opt_str_param(description, "description"),
            constraints=cast(
                "TableFieldConstraints",
                check.opt_inst_param(
                    constraints,
                    "constraints",
                    TableFieldConstraints,
                    default=_DefaultTableFieldConstraints,
                ),
            ),
        )


# ########################
# ##### TABLE FIELD CONSTRAINTS
# ########################


class _TableFieldConstraintsSerializer(DefaultNamedTupleSerializer):
    pass


@whitelist_for_serdes(serializer=_TableFieldConstraintsSerializer)
class TableFieldConstraints(
    NamedTuple(
        "TableFieldConstraints",
        [
            ("required", bool),
            ("unique", bool),
            ("min_length", Optional[int]),
            ("max_length", Optional[int]),
            ("minimum", Optional[object]),
            ("maximum", Optional[object]),
            ("pattern", Optional[str]),
            ("enum", Optional[List[object]]),
            ("other", Optional[List[str]]),
        ],
    )
):
    """Descriptor for a table field's constraints. Properties are derived from the
    `Frictionless Table Constraints
    Descriptor<https://specs.frictionlessdata.io//table-schema#constraints>`.
    An extra property `other` is also supported, which contains a list of
    strings describing arbitrary constraints not captured by the predefined
    properties.

    Args:
        required (Optional[bool]): Indicates whether this field cannot be null. If
          required is false (the default), then null is allowed.
        unique (Optional[bool]): If true, then all values for that field MUST be unique
            within the table.
        min_length (Optional[int]): An integer that specifies a minimum length
          for the field.
        max_length (Optional[int]): An integer that specifies a maximum length
          for the field.
        minimum (Optional[Any]): A minimum value for the field. Type depends on
          the field's data type.
        maximum (Optional[Any]): A maximum value for the field. Type depends on
          the field's data type.
        pattern (Optional[str]): A regular expression that can be used to test field values.
            If the regular expression matches then the value is valid. The values of this
            field MUST conform to the standard `XML Schema regular expression
            syntax<http://www.w3.org/TR/xmlschema-2/#regexs>`.
        enum (Optional[List[Any]]): If supplied, the value of the field must
            match one of the values in the list.
        other (List[str]): Descriptions of arbitrary field-level constraints
            not expressible by the predefined properties.
    """

    def __new__(
        cls,
        required: bool = False,
        unique: bool = False,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        minimum: Optional[object] = None,
        maximum: Optional[object] = None,
        pattern: Optional[str] = None,
        enum: Optional[List[object]] = None,
        other: Optional[List[str]] = None,
    ):
        return super(TableFieldConstraints, cls).__new__(
            cls,
            required=check.bool_param(required, "required"),
            unique=check.bool_param(unique, "unique"),
            min_length=check.opt_int_param(min_length, "min_length"),
            max_length=check.opt_int_param(max_length, "max_length"),
            minimum=minimum,
            maximum=maximum,
            pattern=check.opt_str_param(pattern, "pattern"),
            enum=check.opt_nullable_list_param(enum, "enum"),
            other=check.opt_list_param(other, "other"),
        )


_DefaultTableFieldConstraints = TableFieldConstraints()
