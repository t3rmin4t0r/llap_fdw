from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn import ANY, ALL
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

from pyhive import hive as llap

class LlapConnection(object):
	def __init__(self, hostname='localhost', port=10000, schema='default', username='anonymous', **kwargs):
		basic_conf = {'hive.cli.print.header' : 'false'}
		self.conn = llap.connect(host=hostname, port=int(port), username=username, database=schema, configuration=basic_conf) 
	def list_tables(self):
		cur = self.conn.cursor()
		try:
			cur.execute("show tables")
			for tbl in cur.fetchall():
				yield tbl[0]
		finally:
			cur and cur.close()
	def cursor(self):
		return self.conn.cursor()
	def list_columns(self, table):
		""" return a tuple of (name, type, display_size, None, precision, scale, true) for every col in table """
		cur = self.conn.cursor()
		try:
			cur.execute("select * from %s limit 0" % table)
			for c in cur.description:
				l = list(c)
				l[0] = l[0][len(table)+1:]
				yield tuple(l)
		finally:
			cur and cur.close()
	def convert_coltype(self, col):
		_type_map = {
	     "BOOLEAN_TYPE" : "boolean",
	     "TINYINT_TYPE" : "smallint",
	     "SMALLINT_TYPE" : "smallint",
	     "INT_TYPE" : "int",
	     "BIGINT_TYPE" : "bigint",
	     "FLOAT_TYPE" : "float4",
	     "DOUBLE_TYPE" : "float8",
	     "STRING_TYPE" : "text",
	     "TIMESTAMP_TYPE" : "timestamp",
	     "BINARY_TYPE" : "bytea",
	     "ARRAY_TYPE" : "json",
	     "MAP_TYPE" : "json",
	     "STRUCT_TYPE" : "json",
#	     "UNIONTYPE_TYPE" : "",
	     "DECIMAL_TYPE" : "numeric",
#	     "NULL_TYPE" : "",
	     "DATE_TYPE" : "date",
	     "VARCHAR_TYPE" : "varchar",
	     "CHAR_TYPE" : "char",
#	     "INTERVAL_YEAR_MONTH_TYPE" : "",
#	     "INTERVAL_DAY_TIME_TYPE" : "",
		}
		(name, _type, size, _, precision, scale, _) = col
		if (_type in _type_map):
			_type = _type_map[_type]
			if (size):
				_type += "(%d)" % size
			if (precision):
				_type += "(%d,%d)" % (precision, scale)
			return ColumnDefinition(name, type_name=_type) 
		else:
			log_to_postgres('Cannot handle type %s' % _type)


def to_sarg(q):
	easy_quals = ['=', '>', '>=', '<', '<=', '<>']
	quote = lambda target : isinstance(target, str) or isinstance(target, unicode)
	if q.operator in easy_quals:
		return ("`%s` %s %%s" % (q.field_name, q.operator), q.value) 
	return None

	
def to_sargs(quals):
	log_to_postgres(str(quals), WARNING)
	good_quals = ['=', '>', '>=', '<=', '<>', ('=', True), ('<>',  False)]
	converted = [to_sarg(q) for q in quals if q.operator in good_quals]
	sargs = " and " .join(["(%s)" % a[0] for a in converted if a])
	params = [a[1] for a in converted if a]
	return (sargs, params)
	

class LlapFdw(ForeignDataWrapper):
	def __init__(self, fdw_options, fdw_columns):
		super(LlapFdw, self).__init__(fdw_options, fdw_columns)
		required_params = set(['table','schema','hostname'])
		for p in required_params:
			if p not in fdw_options:
				log_to_postgres('The %s parameter is required' % p, ERROR)
		self.conn = LlapConnection(**fdw_options)
		self.cols = fdw_columns
		self.options = fdw_options
	def build_query(self, quals, columns, sortkeys=None):
		source = self.options["table"]
		query = "select %s from `%s` " % ( ",".join(map(lambda a : '`%s`' % a, columns)), source)
		sargs, params = to_sargs(quals)
		if (sargs):
			query += " where %s" % (sargs) 
		log_to_postgres(query, WARNING)
		return query,params
	def explain(self, quals, columns, sortkeys=None, verbose=False):
		q,p = self.build_query(quals, columns, sortkeys=None)
		if p:
			return [q % llap.HiveParamEscaper().escape_args(p)]
		else:
			return [q]
	def execute(self, quals, columns, sortkeys=None):
		query,params = self.build_query(quals, columns, sortkeys)
		cur = self.conn.cursor()
		try:
			cur.execute(query,parameters=params)
			for r in cur.fetchall():
				yield dict(zip(columns,r))
		finally:
			cur.close()
	
	@classmethod
	def import_schema(self, schema, srv_options, options,
		                      restriction_type, restricts):
		log_to_postgres('We are attemping to import %s ' % schema, WARNING)
		conn = LlapConnection(schema=schema,**srv_options)
		try:
			to_import = []
			for table in conn.list_tables():
				ftable = TableDefinition(table)
				ftable.options['schema'] = schema
				ftable.options['table'] = table
				for c in conn.list_columns(table):
					 ftable.columns.append(conn.convert_coltype(c))
				to_import.append(ftable)
			return to_import
		finally:
			conn or conn.close()

def main(args):

	c = LlapConnection(host='cn105-10', port=10003, schema='tpcds_bin_partitioned_orc_200')
	for t in c.list_tables():
		print t, list(c.list_columns(t))

if __name__ == '__main__':
	import sys
	main(sys.argv[1:])
# vim: ts=4 noet sw=4 ai
