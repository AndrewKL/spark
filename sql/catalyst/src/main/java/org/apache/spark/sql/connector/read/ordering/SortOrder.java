package org.apache.spark.sql.connector.read.ordering;

public class SortOrder {
    public final String column;
    public final SortDirection sortDirection;
    public final NullOrdering nullOrdering;

    public SortOrder(String column, SortDirection sortDirection, NullOrdering nullOrdering) {
        this.column = column;
        this.sortDirection = sortDirection;
        this.nullOrdering = nullOrdering;
    }

    public static final SortDirection DefaultSMJSortDirection = SortDirection.Asc;
    public static final NullOrdering DefaultSMJNullOrdering = NullOrdering.NullsFirst;
}

