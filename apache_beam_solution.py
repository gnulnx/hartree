#!/usr/bin/env python
import apache_beam as beam
import typing

# SQL
from apache_beam.transforms.sql import SqlTransform

print("Working")
class DS1Schema(typing.NamedTuple):
    """
    Scheme to hold data form dataset2.csv
    # invoice_id,legal_entity,counter_party,rating,status,value
    """
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    val: int

    @classmethod
    def to_dict(cls, x):
        """
        Helper method to convert csv rows into json for insertion to schema
        NOTE: I'm sure there must be a better way!  
        """
        vals = x.split(",")
        return {
            "invoice_id": int(vals[0]),
            "legal_entity": str(vals[1]),
            "counter_party": str(vals[2]),
            "rating": int(vals[3]),
            "status": str(vals[4]),
            "val": int(vals[5])
        }
beam.coders.registry.register_coder(DS1Schema, beam.coders.RowCoder)
print("schema defined")

class DS2Schema(typing.NamedTuple):
    """
    Scheme to hold data form dataset2.csv
    # counter_party,tier
    """
    counter_party: str
    tier: int

    @classmethod
    def to_dict(cls, x):
        """
        Helper method to convert csv rows into json for insertion to schema
        NOTE: I'm sure there must be a better way!  
        """
        vals = x.split(",")
        return {
            "counter_party": str(vals[0]),
            "tier": int(vals[1]),
        }
beam.coders.registry.register_coder(DS2Schema, beam.coders.RowCoder)

# Build Pipline
with beam.Pipeline() as pipeline:
    ds1 = (
        pipeline 
        | "Read dataset1" >> beam.io.ReadFromText("/Users/jfurr/hartree/dataset1.csv", skip_header_lines=True)
        | "Convert ds1 to json" >> beam.Map(DS1Schema.to_dict)
        | beam.Map(lambda x: DS1Schema(**x)).with_output_types(DS1Schema)
    )
  
    ds2 = (
        pipeline 
        | "Read dataset2" >> beam.io.ReadFromText("/Users/jfurr/hartree/dataset2.csv", skip_header_lines=True)
        | "Convert ds2 to json" >> beam.Map(DS2Schema.to_dict)
        | beam.Map(lambda x: DS2Schema(**x)).with_output_types(DS2Schema)
    )

    # This is actually working!!!
    # Just needs "Also create new record to add total for each of legal entity, counterparty & tier.""
    (
        {'ds1': ds1, 'ds2': ds2}
        | SqlTransform("""
            SELECT 
                ds1.legal_entity,
                ds1.counter_party,
                ds2.tier,
                max(ds1.rating) as max_rating_counter_party,
                CASE
                    WHEN arap.total is null THEN 0
                    ELSE arap.total
                END arap_total,
                CASE
                    WHEN accr.total is null THEN 0
                    ELSE accr.total
                END accr_total,
                count(*) as total
            FROM ds1
            INNER JOIN ds2 ON ds1.counter_party = ds2.counter_party 

            LEFT JOIN (
                SELECT ds1.legal_entity, ds1.counter_party, sum(ds1.val) as total
                FROM ds1
                WHERE ds1.status = 'ARAP'
                GROUP BY ds1.legal_entity, ds1.counter_party
            ) arap 
            ON ds1.counter_party = arap.counter_party 
            AND ds1.legal_entity = arap.legal_entity

            LEFT JOIN (
                SELECT ds1.legal_entity, ds1.counter_party, sum(ds1.val) as total
                FROM ds1
                WHERE ds1.status = 'ACCR'
                GROUP BY ds1.legal_entity, ds1.counter_party
            ) accr 
            ON ds1.counter_party = accr.counter_party 
            AND ds1.legal_entity = accr.legal_entity
            
            GROUP BY ds1.legal_entity, ds1.counter_party, ds2.tier, arap.total, accr.total
            ORDER BY ds1.legal_entity, ds1.counter_party LIMIT 1000
        """)
        | beam.Map(lambda row: ','.join(map(str, row)))
        | beam.io.WriteToText(
            "apache_beam_results",
            ".csv",
            shard_name_template='',
            header="legal_entity,counter_party,tier,max_rating_counter_party,total_value_for_ARAP,total_value_for_ACCR,total_members_in_group"
        )
        # | beam.Map(print)
    )