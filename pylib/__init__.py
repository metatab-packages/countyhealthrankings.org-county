""" Example pylib functions"""

from geoid.acs import County


def row_generator(resource, doc, env, *args, **kwargs):
    """ This file has descriptions of the columns on the first row, 
    and sensible colum names on the second. This generate will extract the descirptions
    and link them into the schema. 
    """

    source = doc.reference('source')

    rows = iter(source)
    
    descriptions = next(rows)# remove the first line
    
    yield ['geoid'] + next(rows)

    for row in rows:
        yield [County(row[0],row[1])] + row
    

def example_transform(v, row, row_n, i_s, i_d, header_s, header_d,scratch, errors, accumulator):
    """ An example column transform.

    This is an example of a column transform with all of the arguments listed. An real transform
    can omit any ( or all ) of these, and can supply them in any order; the calling code will inspect the
    signature.

    When the function is listed as a transform for a column, it is called for every row of data.

    :param v: The current value of the column
    :param row: A RowProxy object for the whiole row.
    :param row_n: The current row number.
    :param i_s: The numeric index of the source column
    :param i_d: The numeric index for the destination column
    :param header_s: The name of the source column
    :param header_d: The name of the destination column
    :param scratch: A dict that can be used for storing any values. Persists between rows.
    :param errors: A dict used to store error messages. Persists for all columns in a row, but not between rows.
    :param accumulator: A dict for use in accumulating values, such as computing aggregates.
    :return: The final value to be supplied for the column.
    """

    return str(v)+'-foo'

def custom_update(doc, args):
    from metapack.cli.core import prt
    
    source = doc.reference('source')

    r = iter(source)
    
    descriptions = next(r)
    header = next(r)
    
    desc_map = {k:v for k, v in zip(header, descriptions)}
    
    changes = 0 
    for c in doc['Schema'].find('Table.Column'):
        desc = desc_map.get(c.name)
        
        if desc and c.get_value('Description') != desc:
            prt(f"Setting {c.name} description to: {desc}")
            c['Description'] = desc
            changes += 1
            
        if c.get_value('Datatype') == 'unknown':
            c['Datatype'] = 'number'
            changes += 1
            
            
    if changes:
        prt(f'Writing {changes} changes')
        doc.write()
    else:
        prt('No changes')