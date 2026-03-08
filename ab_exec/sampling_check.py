from prettytable import PrettyTable
from scipy import stats


def chi_squared_pvalue(counts):
    _, pval = stats.chisquare([x / sum(counts) for x in counts], [1 / len(counts)] * len(counts))
    return pval


def print_sampling_check(query_results):
    groups, counts = zip(query_results)
    groups = list(groups)
    counts = list(counts)
    table = PrettyTable()
    table.field_names = groups
    table.add_row(counts)

    pval = chi_squared_pvalue(counts)

    print(f"""
Groups have the following user distribution:
{table}
P-value of this distribution being uniform: {pval * 100:.2f}% 
""".strip())

