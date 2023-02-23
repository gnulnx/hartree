#!/usr/bin/env python

import pandas as pd

df1 = pd.read_csv("dataset1.csv")
df2 = pd.read_csv('dataset2.csv')

# Inner join dataframes on counter_party
join_results = df1.merge(df2, on="counter_party", how="inner")

def out_fn(x):
    """
    function used to handle multiple aggregations where some also need query clauses
    """
    d = {}
    d["max_rating_counter_party"] = max(x.rating)
    d["total_value_for_ARAP"] = sum(x.query("status == 'ARAP'")["value"])
    d["total_value_for_ACCR"] = sum(x.query("status == 'ACCR'")["value"])
    d["total_members_in_group"] =  len(x)
    return pd.Series(d)

# Create output datafrom
# legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
#
# NOTE:  I found this part of the test question a little confusing.
#   * 'Also create new record to add total for each of legal entity, counterparty & tier.'
# I ended up adding an extra field to the output (total_members_in_group) that provided a count for each groupby.
# In a live interview I would have asked questions instead of assuming.  ;)
out = join_results.groupby(["legal_entity", "counter_party", "tier"], as_index=False).apply(out_fn)

# Write output to csv file
out.to_csv("pandas_results.csv", index=False)
