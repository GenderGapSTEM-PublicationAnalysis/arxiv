TABLE_ARTICLE = 'arxiv_articles'
TABLE_AUTHORSHIP = 'arxiv_authorship'
TABLE_AFFILIATION = 'arxiv_affiliations'

COLUMNS_ARTICLES = ['identifier', 'title', 'created', 'categories', 'datestamp', 'set_spec', 'abstract',
                    'msc_class', 'acm_class', 'comments', 'updated', 'journal_ref', 'report_no', 'doi']
COLUMNS_AUTHORSHIP = ['article_id', 'author_pos', 'keyname', 'forenames', 'suffix']
COLUMNS_AFFILIATIONS = ['article_id', 'author_pos', 'affiliation']
