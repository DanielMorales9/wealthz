import polars as pl
import pytest
from polars.polars import ColumnNotFoundError, InvalidOperationError

from wealthz.model import (
    CastTransform,
    Column,
    ColumnType,
    DateFormatTransform,
    RegexReplaceTransform,
    SplitTransform,
    SubstringTransform,
    TrimTransform,
    UpperTransform,
)
from wealthz.transforms import (
    CastColumnTransform,
    ColumnTransformer,
    LowerColumnTransform,
    RegexReplaceColumnTransform,
    SplitColumnTransform,
    SubstringColumnTransform,
    TrimColumnTransform,
    UpperColumnTransform,
)


def test_cast_transform():
    transform = CastColumnTransform()
    expr = pl.col("test")

    # Test string to integer
    params = CastTransform(target_type=ColumnType.INTEGER)
    result = transform.apply(expr, params)
    df = pl.DataFrame({"test": ["1", "2", "3"]}).select(result.alias("test"))
    assert df.dtypes[0] == pl.Int32
    assert df["test"].to_list() == [1, 2, 3]


def test_trim_transform():
    transform = TrimColumnTransform()
    expr = pl.col("test")

    result = transform.apply(expr, TrimTransform())
    df = pl.DataFrame({"test": ["  hello  ", " world ", "test"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["hello", "world", "test"]


def test_upper_lower_transforms():
    upper_transform = UpperColumnTransform()
    lower_transform = LowerColumnTransform()
    expr = pl.col("test")

    # Test uppercase
    result = upper_transform.apply(expr, {})
    df = pl.DataFrame({"test": ["hello", "World", "TEST"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["HELLO", "WORLD", "TEST"]

    # Test lowercase
    result = lower_transform.apply(expr, {})
    df = pl.DataFrame({"test": ["HELLO", "World", "test"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["hello", "world", "test"]


def test_regex_replace_transform():
    transform = RegexReplaceColumnTransform()
    expr = pl.col("test")

    params = RegexReplaceTransform(pattern=r"\d+", replacement="X")
    result = transform.apply(expr, params)
    df = pl.DataFrame({"test": ["abc123def", "456xyz", "no numbers"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["abcXdef", "Xxyz", "no numbers"]


def test_split_transform():
    transform = SplitColumnTransform()
    expr = pl.col("test")

    # Test default index (0)
    result = transform.apply(expr, SplitTransform(delimiter=","))
    df = pl.DataFrame({"test": ["a,b,c", "x,y", "single"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["a", "x", "single"]

    # Test specific index
    result = transform.apply(expr, SplitTransform(delimiter=",", index=1))
    df = pl.DataFrame({"test": ["a,b,c", "x,y", "single"]}).select(result.alias("test"))
    expected = ["b", "y", None]  # Polars returns None for out-of-bounds
    actual = df["test"].to_list()
    assert actual == expected


def test_substring_transform():
    transform = SubstringColumnTransform()
    expr = pl.col("test")

    # Test start only
    result = transform.apply(expr, SubstringTransform(start=2))
    df = pl.DataFrame({"test": ["hello", "world", "hi"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["llo", "rld", ""]

    # Test start and length
    result = transform.apply(expr, SubstringTransform(start=1, length=3))
    df = pl.DataFrame({"test": ["hello", "world", "hi"]}).select(result.alias("test"))
    assert df["test"].to_list() == ["ell", "orl", "i"]


def test_apply_transforms_simple():
    # Define columns with transforms
    columns = [
        Column(
            name="name",
            type=ColumnType.STRING,
            transforms=[
                TrimTransform(),
                UpperTransform(),
            ],
        ),
        Column(
            name="age",
            type=ColumnType.INTEGER,
            transforms=[
                CastTransform(target_type=ColumnType.INTEGER),
            ],
        ),
        Column(
            name="city",
            type=ColumnType.STRING,
            transforms=[
                UpperTransform(),
            ],
        ),
    ]

    engine = ColumnTransformer(columns)

    # Create test data
    df = pl.DataFrame({
        "name": ["  john  ", "  JANE  ", "  bob  "],
        "age": ["25", "30", "35"],
        "city": ["new york", "los angeles", "chicago"],
    })

    # Apply transforms
    result = engine.transform(df)

    # Verify results
    assert result["name"].to_list() == ["JOHN", "JANE", "BOB"]
    assert result["age"].to_list() == [25, 30, 35]
    assert result["age"].dtype == pl.Int32
    assert result["city"].to_list() == ["NEW YORK", "LOS ANGELES", "CHICAGO"]


def test_apply_transforms_with_missing_columns():
    # Define columns including non-existent one
    columns = [
        Column(name="existing", type=ColumnType.STRING, transforms=[UpperTransform()]),
        Column(name="missing", type=ColumnType.STRING, transforms=[UpperTransform()]),
    ]

    engine = ColumnTransformer(columns)

    # Create test data
    df = pl.DataFrame({"existing": ["test"]})

    with pytest.raises(ColumnNotFoundError, match="missing"):
        result = engine.transform(df)
        assert result["existing"].to_list() == ["TEST"]
        assert "missing" not in result.columns


def test_apply_transforms_order():
    # Define columns with transforms in specific order
    columns = [
        Column(
            name="test",
            type=ColumnType.STRING,
            transforms=[
                UpperTransform(),
                TrimTransform(),
                SplitTransform(delimiter=",", index=0),
            ],
        ),
    ]

    engine = ColumnTransformer(columns)

    # Create test data
    df = pl.DataFrame({"test": ["  hello,world  "]})

    # Apply transforms
    result = engine.transform(df)

    # Should be: trim -> upper -> split
    # "  hello,world  " -> "hello,world" -> "HELLO,WORLD" -> "HELLO"
    assert result["test"].to_list() == ["HELLO"]


def test_transform_error_handling():
    # Define columns with invalid transform
    columns = [
        Column(
            name="test",
            type=ColumnType.INTEGER,
            transforms=[
                CastTransform(target_type=ColumnType.INTEGER),
            ],
        ),
    ]

    engine = ColumnTransformer(columns)

    # Create test data
    df = pl.DataFrame({"test": ["invalid"]})

    # Should raise TransformError
    with pytest.raises(
        InvalidOperationError, match="conversion from `str` to `i32` failed in column 'test' for 1 out of 1 values:"
    ):
        engine.transform(df)


def test_no_transforms():
    # Define columns without transforms
    columns = [Column(name="test", type=ColumnType.STRING, transforms=[])]

    engine = ColumnTransformer(columns)

    # Create test data
    df = pl.DataFrame({"test": ["hello", "world"]})

    # Apply transforms
    result = engine.transform(df)

    # Should be unchanged
    assert result.equals(df)


def test_empty_columns():
    engine = ColumnTransformer([])

    # Create test data
    df = pl.DataFrame({"test": ["hello", "world"]})

    # Apply transforms with empty columns list
    result = engine.transform(df)

    # Should be unchanged
    assert result.equals(df)


def test_financial_data_transforms():
    """Test transforms on financial transaction data."""
    # Define comprehensive transforms
    columns = [
        Column(
            name="Date",
            type=ColumnType.DATE,
            transforms=[
                TrimTransform(),
                DateFormatTransform(input_format="%m/%d/%Y"),
            ],
        ),
        Column(
            name="Symbol",
            type=ColumnType.STRING,
            transforms=[
                TrimTransform(),
                UpperTransform(),
            ],
        ),
        Column(
            name="Quantity",
            type=ColumnType.INTEGER,
            transforms=[
                CastTransform(target_type=ColumnType.INTEGER),
            ],
        ),
        Column(
            name="Price",
            type=ColumnType.FLOAT,
            transforms=[
                RegexReplaceTransform(pattern=r"(\$|,)"),
                CastTransform(target_type=ColumnType.FLOAT),
            ],
        ),
        Column(
            name="Type",
            type=ColumnType.STRING,
        ),
        Column(
            name="Fees",
            type=ColumnType.FLOAT,
            transforms=[
                RegexReplaceTransform(pattern="N/A", replacement="0"),
                RegexReplaceTransform(pattern=r"(\$|,)"),
                CastTransform(target_type=ColumnType.FLOAT),
            ],
        ),
    ]
    engine = ColumnTransformer(columns)

    # Create sample financial data
    df = pl.DataFrame({
        "Date": ["1/15/2024", "2/20/2024", "3/10/2024"],
        "Symbol": ["  AAPL  ", "  googl  ", "  MSFT  "],
        "Quantity": ["100", "50", "200"],
        "Price": ["$150.50", "$2,800.00", "$400.25"],
        "Type": ["BUY", "SELL", "BUY"],
        "Fees": ["$9.1", "N/A", "$12.50"],
    })

    # Apply transforms
    result = engine.transform(df)

    # Verify results
    assert result["Symbol"].to_list() == ["AAPL", "GOOGL", "MSFT"]
    assert result["Quantity"].to_list() == [100, 50, 200]
    assert result["Quantity"].dtype == pl.Int32
    assert result["Price"].to_list() == [150.50, 2800.00, 400.25]
    assert result["Type"].to_list() == ["BUY", "SELL", "BUY"]
    assert result["Fees"].to_list() == [9.100000381469727, 0.0, 12.5]
