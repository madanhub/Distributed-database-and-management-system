class ColumnDataType:
    def __init__(self, dataType: str, length: int, isAllowNull:  bool, isAllowUnique: bool, isPrimaryKey: bool, isForeignKey: bool, fkConstraintName: str):
        self.dataType = dataType
        self.length = length
        self.isAllowNull = isAllowNull
        self.isAllowUnique = isAllowUnique
        self.isPrimaryKey = isPrimaryKey
        self.isForeignKey = isForeignKey
        self.fkConstraintName = fkConstraintName

class ColumnData:
    def __init__(self, columnName: str, columnData: ColumnDataType):
        self.columnName = columnName
        self.columnData = columnData