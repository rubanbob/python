import re
import shlex

def col_convert(col):
    col = col.encode('ascii','ignore').decode('utf-8').lower()
    col = col.replace(' ', '_')
    col = re.sub('\\W+', '', col)
    col = re.sub('^(\\d)', 'col_\\1', col)
    col = re.sub('^_', 'col_', col)
    return col

def replace_in_string_outside_quotes(s, target, replacement):
    # This pattern matches either a quoted string or the target word
    # target.replace(' ','\s+') is used to match the target word with spaces in between (e.g. 'is   no null')
    pattern = r'(?<!\\)"(.*?)(?<!\\)"|\b' + target.replace(' ','\s+') + r'\b'
    #print("Pattern : {}".format(pattern))
    def replacer(match):
        # If the match is a quoted string, return it unchanged
        if match.group(1) is not None:
            return match.group(0)
        # Otherwise, return the replacement string
        else:
            return replacement

    # re.IGNORECASE - ignore case and re.DOTALL - make . match newlines
    return re.sub(pattern, replacer, s, flags=re.IGNORECASE | re.DOTALL)

# add case and error message to expresssion received on qc
def convert_to_case(column_names, in_expression, error_msg, errcol):

    # sorting based on length - so replace of substing column can be avoided
    length_sorted_cols = sorted(column_names, key=len, reverse=True)
    converted_cols =[col_convert(item) for item in length_sorted_cols]

    # doing dual replace - if once col_name  is substring of another col_name it will be difficult to convert
    for ind, char in enumerate(length_sorted_cols):
        in_expression = in_expression.replace(f'`{char}`',f'{{{ind}}}') # replace column names within `` to corresponding index no

    print("Expression after replacing column names with index : {}".format(in_expression))
    in_expression = replace_in_string_outside_quotes(in_expression, 'is not null', ' != "" ')
    in_expression = replace_in_string_outside_quotes(in_expression, 'is null', ' = "" ')
    #print("Expression after replacing is null and is not null : {}".format(in_expression))

    if errcol:
        # use colum names provided by user
        lexer = shlex.shlex(errcol)
        lexer.quotes = '`'
        lexer.whitespace = ","
        tet = list(lexer)

        qc_err_col = ",".join([col_convert(i) for i in tet])
    else:
        # get only column names used in qc 
        get_index_pattern = re.compile(r"\{(\d+)\}")
        col_in_qc_list = get_index_pattern.findall(in_expression)
        col_in_qc_set = set(col_in_qc_list)
        qc_err_col = ",".join([converted_cols[int(i)].strip('"') for i in col_in_qc_set])
    print("Columns to report errors : {}".format(qc_err_col))

    for ind, replacement in enumerate(converted_cols):
        in_expression = in_expression.replace(f'{{{ind}}}',f'`{replacement}`') # re-replace index with actual column name

    # if user has not mentioned any custom error message for rules
    if not error_msg:
        error_msg = "QC failed"

    error_msg_with_rowno = f'concat(\'"{error_msg}":["\',_rowno,\':{qc_err_col}"]\') '
    added_case = 'CASE WHEN ' + in_expression + ' THEN NULL \n ELSE ' + "error_msg_with_rowno" + ' \n END'

    print("CASE converted Rule : {}".format(added_case))
    return added_case

in_expression = """(`Address` IN ("North St (1st
 Block)")) AND (`STATE` IN ("CA")) AND (`ZIP` IS NULL)) AND (`NOP` IN ("# of (1st
 Block)))"))
"""
# Extract column names using a regular expression
column_names = re.findall(r'`(\w+)`', in_expression)

# Remove duplicates and sort the column names
column_names = sorted(set(column_names))

# Create the errcol string
errcol = ", ".join(column_names)

error_msg = "Test failed"

convert_to_case(column_names, in_expression, error_msg, errcol)
