<?php






function GENERIC_DATABASE_FUNCTIONS(){}



function get_puppet_hosts_by_role( $_role = "mysql" ) { 

	
	static $_arr_x;

	if( isset($_arr_x) ) return $_arr_x;
	

	$_cn2 = mysql_connect( "mysql-db1" , "idealo" , "ALg0SedZKa" , true , MYSQL_CLIENT_COMPRESS );

	if( $_cn2 ){ 
		if( mysql_select_db( "puppet", $_cn2 ) ){ 

		} else { 
			echo "Couldn't find DB 'puppet' on host 'mysql-db1' <BR>\n";
			return NULL;
		} 



		$_q = "
			/* puppet mysqld hosts 
			von s109 auslesen */
			select  h.name hostname, h.environment 
					, fn.name
					, fv.*
			from 	puppet.fact_values fv
					inner join puppet.hosts h       ON fv.host_id      = h.id
					inner join puppet.fact_names fn ON fv.fact_name_id = fn.id
			where (  	fv.host_id in ( 333694, 'some_host_id')
						or TRIM(LOWER(value)) = '{$_role}'
					)
			and fv.fact_name_id = 427 
			order by hostname
			;
		";

		$_result1 = mysql_query($_q);
		if( $_result1 === false ){
			print_error( "query error<BR>\n" );
		}		


		$_arr_return = mysql_fetch_rows_arr( $_result1  );
		return($_arr_return);


	} else { 
		print_error("puppet db connect error: ".mysql_errno().": ".mysql_error()."<BR>");
		return NULL;
	} 

} 


function reconnect($_new_host, $_new_db = ""){
	
	$_REQUEST["hostname"] = $_new_host;


	global $DB_CONNECTION;
	$DB_CONNECTION = false;


	if($_new_db){
		global $PUB_DB_NAME;
		$PUB_DB_NAME = $_new_db;
	}


}

function connectPublicDB() { 

	

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require

	if( ! $DB_CONNECTION ){ 

		$DB_CONNECTION = mysql_connect( $PUB_DB_HOST , $DB_USER , $DB_PASSWORD , false , MYSQL_CLIENT_COMPRESS );
		$_MYSQL_THREAD_ID = mysql_thread_id($DB_CONNECTION);
		#vd("MYSQL_THREAD_ID = $_MYSQL_THREAD_ID");

		/* Create a new mysqli object with database connection parameters */
		$MYSQLI = new mysqli($PUB_DB_HOST_MYSQLI , $DB_USER, $DB_PASSWORD, $PUB_DB_NAME);
		if( mysqli_connect_errno() ){
			echo "mysqli connection failed: " . mysqli_connect_errno();
			#exit();
		} else {
			#echo "mysqli connection established to $PUB_DB_HOST , $DB_USER<br>\n ";

		}



		if( $DB_CONNECTION ){ 

			$_supports = 	mysql_set_charset(  $CONNECTION_CHARSET , $DB_CONNECTION );
			if( ! $_supports ) "Zeichensatz $CONNECTION_CHARSET konnte nicht gesetzt werden.<br>";
			mysql_query("SET character_set_results = '{$CONNECTION_CHARSET}', character_set_client = '{$CONNECTION_CHARSET}', character_set_connection = '{$CONNECTION_CHARSET}', character_set_database = '{$CONNECTION_CHARSET}', character_set_server = '{$CONNECTION_CHARSET}'", $DB_CONNECTION);

			if(   $PUB_DB_NAME  ){ 
				if(   mysql_select_db( $PUB_DB_NAME, $DB_CONNECTION ) ){ 
					return $DB_CONNECTION; 
				} else { 
					echo "Couldn't find DB $PUB_DB_NAME on host $PUB_DB_HOST<BR>";
					return NULL;
				} 
			}	else {
				return $DB_CONNECTION; 				
			}
			

				
		} else { 
			echo "Database Connect Error: ".mysql_errno().": ".mysql_error()."<BR>";
			return NULL;
		} 

	} else {
		return $DB_CONNECTION;
	}


		
} 
 



/*
	centrally control the connection behaviour.
	If we keep a connection open,
	MySQL-PHP reuses the connection for another db-request
	within the same php-http-request.
*/
function mysql_custom_close($link){	
	# mysql_close($link);	# currently we keep it open
};




/* 	get_bulk_loaded_data uses mysql_fetch_rows_arr
		and puts the result into an array with request scope.
		
		The key for the request-scope array is 
			$_REQUEST['request_cache'][ querystring - by primary key columnn name ]
		
		it resembles a mysql query cache - 
		but reduces network traffic by fetching all required data
		at ONCE over network, and then returning them from
		application server memory.
		
		It encourages bundling and preloading
		of data for any list-views.
		

 */
function get_bulk_loaded_data($_public_or_priv, $_query, $_pk_name, $_pk_value) {
	

	$_cache_key = "$_query - by $_pk_name";
	
	# echo "cache_key $_cache_key<br>";

	$_arr_data = $_REQUEST['request_cache'][$_cache_key];

	
	if( is_array($_arr_data)  AND sizeof($_arr_data) > 0  ) {
		# array already in request memory;
	} else {


		$link=connectPublicDB();
		
		if( $link ){
			$q = $_query;
			#echo "DBG: q is " . $q . "<BR>";
			#vd($q);	
			#debug_print_backtrace();	
			$_query_res =mysql_query( $q) ;	
			$_arr_data = mysql_fetch_rows_arr($_query_res, $_pk_name);
			#vd($_arr_data);

		}

		$_REQUEST['request_cache'][$_cache_key] = $_arr_data ;
		
		mysql_custom_close($link);
	}


	if ( 			! $_pk_value  
				OR	! $_arr_data[$_pk_value]
	) {
		return array();
	} else {
		#vd($_arr_data[$_pk_value]);
		return $_arr_data[$_pk_value];	
	}


}



















# http://de2.php.net/manual/de/function.addslashes.php
#	replace addslashes by mysql_real_escape_string
#	conditioned by get_magic_quotes_gpc
#	functio escape_for_sql($_str){
#	functio unescape_from_post($_str){

/*
		Helper function for mysql_fetch_array()
		when accessing data from SEVERAL rows. 
		
		It is practical when more than one row is expected to be returned from a SELECT query. 
		
		It returns a 2-dimensional array in the form of 
		$arr[primary_key_value][column_name] = column_value. 
		
		Additionally, you can access the array by numeric index!
		If there is some column 'password', and you want the password from the THIRD returned row:
			$arr = mysql_fetch_rows_arr($result);
			$third_password = $arr[2]['password'];
			
		If no parameter $_primary_key_col is given, the array is organized by numerical index only.
		
		parameter $result is any MYSQL query result.
			
*/
function mysql_fetch_rows_arr($result, $_primary_key_col = false  ){

		$got = array();

		# getting the columnn names
		
		#debug_print_backtrace();
		
		$_first_row = mysql_fetch_array($result, MYSQL_ASSOC);
		if( $_first_row ){
			$keys = array_keys( $_first_row );
			mysql_data_seek($result, 0);
	
			# building the array based on column names
			$_i = 0;

			while ($row = mysql_fetch_array($result, MYSQL_ASSOC) ){
				#vd($row);
				if(			  $_primary_key_col 
						AND !  $row[$_primary_key_col] 
						AND ! ($row[$_primary_key_col] === "0" )
				){
					print_error("value for pk -$_primary_key_col- is not in result set. Results might be condensed. Check the query!");
					if( isset(	$row[$_primary_key_col] ) OR  is_null( $row[$_primary_key_col] ) ){
						print_error("value for pk -$_primary_key_col- is NULL or is empty !");
					} 
					echo "isset : -".	isset( $row[$_primary_key_col] )."-<br>\n";
					echo "isnull: -". is_null( $row[$_primary_key_col]) ."-<br>\n";
					vd($row);
					echo "<pre>";
					debug_print_backtrace(); echo "<br>\n";
					echo "</pre>";
				} 
				foreach ($keys as $_single_key ){					
					if( $_primary_key_col){
						$_pk_val = $row[$_primary_key_col];
						# echo "primary_key_col = $_primary_key_col / row[primary_key_col] = $_pk_val<br>\n";
						$got[$_pk_val][$_single_key]=$row[$_single_key];
					} 
					if( ! $_primary_key_col){
						$got[ $_i					  ][$_single_key]=$row[$_single_key];
					}				
				}
				$_i++;
			}
		}
		
		return $got;

}

#echo "005a<br>";






/*
	To setup an input form, we need all the columns from a database table.
	We read those from database TABLE DEFINITION.
	
	Often, default values are desired - 
	And as default values are often specified at database level,
	we read those from the database TABLE DEFINITION as well.
	They may be overridden as desired, but at least they are not all empty strings, but "0.00" or "0000-00-00".
	
	Argument $_single_table_name_or_array may be a scalar value of one table,
	or an array with tables.
	
	Returns array tablename-col_name-col_default
	
*/
function get_database_columns_with_defaults( $_single_table_name_or_array ){

	connectPublicDB();
	
	# distinguish between single scalar table_name and array of table_names
	if( is_array( $_single_table_name_or_array)  ){
		$_arr_table_names = $_single_table_name_or_array;
	}else{
		if( $_single_table_name_or_array ){
			$_arr_table_names = array( $_single_table_name_or_array );
		}else {
			return array();
		}
	}

	$_table_names_in_clause = implode( "' , '" , $_arr_table_names);
	$_table_names_in_clause = " '$_table_names_in_clause' ";


	$_arr_tablename_colname_defaultval = array();
	global $PUB_DB_HOST, $PUB_DB_USER, $PUB_DB_PW, $PUB_DB_NAME;	
	$_q = "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT 
			FROM information_schema.columns			
			WHERE table_schema='$PUB_DB_NAME' AND table_name IN ( $_table_names_in_clause ) ";
	#echo $_q;
	$_res3 = mysql_query($_q);

	if( $_last_err = mysql_errno() ){
		echo "<span class=error >Datenbankfehler Nr. $_last_err; Meldung: " . mysql_error(). ".</span><br>
		query was <pre> $_q </pre>\n";		
	}else{
		while ($_row3 = mysql_fetch_array($_res3) ){
				$_table_name  = $_row3['TABLE_NAME'];
				$_col_name	= $_row3['COLUMN_NAME'];
				$_col_default = $_row3['COLUMN_DEFAULT'];
				$_arr_tablename_colname_defaultval[$_table_name][$_col_name] = $_col_default;
		}

	}

	mysql_custom_close($link);


	return $_arr_tablename_colname_defaultval;
	
}




function get_mysql_enum_values($table,$field) {

	$_data = execute_query_get_first("SHOW COLUMNS FROM $table LIKE '$field' " );	
	$_str1 = preg_replace( "/(enum|set)\('(.+?)'\)/" , "\\2" , $_data['Type']);
	$options=explode("','", $_str1);
	return $options;
}




/**
*		query execute a query
*		return multiple records in an associative array, 
*		either keyed by primary key or by numerical index.

		returns ALWAYS an array 
*/
function execute_query($_q, $colname_pk=false){

	#echo "pk_col = $colname_pk <br>\n";

	$_arr_return = array();

	$link = connectPublicDB();
	
	#vd($_q);
	

	render_elapsed_time_since( "<none>");
	

	$_result1 = mysql_query($_q);


	if( stripos($_q,"SQL_CALC_FOUND_ROWS")>0 ){
		$_result_obj_anz = mysql_query( "SELECT FOUND_ROWS() " );
		$_arr_anz = mysql_fetch_array( $_result_obj_anz ) ;
		#vd($_arr_anz, 'unlimited rows would be');		

		global $_TOTAL_COUNT;	
		$_TOTAL_COUNT = $_arr_anz[0];
		#vd($_TOTAL_COUNT, 'unlimited rows would be');		
	}


	$_q_for_dis = $_q;
	$_q_for_dis = preg_replace('!/\*.*?\*/!s', '', $_q_for_dis);
	$_q_for_dis = preg_replace('/\n\s*\n/', "\n", $_q_for_dis);
	
	render_elapsed_time_since( $_q_for_dis);

	$_query_info = "";

	$_mysql_info = mysql_info();
	if( $_mysql_info) $_query_info  .= " $_mysql_info , ";

	$_aff_rows = mysql_affected_rows();
	if( $_aff_rows OR $_aff_rows===0) $_query_info  .= "rows_affected: $_aff_rows rows, ";

	if( strlen($_query_info) > 5 ) $_query_info = substr($_query_info,0,-2);

	handle_warnings();
	
	if( $_last_err = mysql_errno() ){
		$_err_msg = mysql_error();
		print_error( "Datenbankfehler1 Nr. $_last_err; Meldung: $_err_msg $_info<br>query was <pre> $_q </pre>\n" );
	}else{

		if( get_param('dbg_show_mysql_warnings')  ){
			#vd( "Query Info: $_query_info  " . substr($_q_for_dis,0,100)  );
			echo "Query Info: $_query_info" ;
		}
		
		# handling the case of an simple update query
		if( $_result1 == 1){
			if( strpos( $_q, " do_log_info ")  ){
				log_message( "Datenbank&auml;nderung ausgef&uuml;hrt  $_info");			
			}
		}
		else if( $_result1 === false ){
			print_error( "No Datenbankfehler, but result === false; $_info<br>query was <pre> $_q </pre>\n" );
		}		
		else {
			# GOOD
			# select query with return data
			#echo "pk_col = $colname_pk <br>\n";
			$_arr_return = mysql_fetch_rows_arr( $_result1 , $colname_pk );
			#vd($_arr_return);
		}
	}
	mysql_custom_close($link);
	
	render_elapsed_time_since( "query result array processed");
	
	
	return $_arr_return;
}


/*
	wrapper um execute_query, wenn man nur genau EIN record zurückerwartet.
	
	Vorsicht: Wenn DOCH mehr als EIN Record zurückkommen kann - 
		die Reihenfolge ist ggf. ZUFÄLLIG ohne ORDER BY clause in der Query
	
*/
function execute_query_get_first( $_q, $colname_pk=false ){
	$_arr_return = execute_query($_q, $colname_pk);
	
	if( is_array( $_arr_return )  AND sizeof( $_arr_return ) ){
		foreach( $_arr_return as $_key => $_one_row){
			$_arr_first_row = $_one_row;
			break;
		}
	}else{
		# NOTHING FOUND - not neccessarily an error - 
		return array();
	}
	
	if( is_array( $_arr_first_row )  AND sizeof($_arr_first_row) ){
		return $_arr_first_row;
	}
	else if( is_array( $_arr_first_row )  AND  ! sizeof($_arr_first_row) ){
		print_error("execute_query_get_first: Erster Key im Resultset ist LEER.");
		vd($_arr_return);
		return array();
	}
	else if( ! is_array( $_arr_first_row )  ){
		print_error("execute_query_get_first: Erster Key im Resultset war kein array.");
		vd($_arr_return);
		return array();
	}else{
		print_error("execute_query_get_first: Sonstiger Fehler mit dem ersten Record.");
		return array();
	}
	
}

# execute query, SAFE from sql injection attacks
#		but requires all the params in separate data structure
#		for TÜV certificate, we will one day have to rewrite all external queries for this :(
function execute_query_p($_query, $_arr_args, $colname_pk=false){
	
	#vd($_arr_args);
	
	$_i = 0;
	$_query_orig = $_query;
	$_place_holder = '?';

	$_count_occ = substr_count($_query,$_place_holder);
	if( $_count_occ <> sizeof($_arr_args) ){
		print_error(" zu wenige Werte im Arg-Array. $_query"); 
		vd($_arr_args);
		return -1;
	} else {
		#vd("$_count_occ mal '$_place_holder' in $_query");	
	}
	
	
	while(
			!	( strpos($_query, $_place_holder ) === FALSE )
		AND	( $_i < 100 )
	){
		
		$_pos = strpos($_query, $_place_holder );
		$_part1 = substr( $_query, 0, $_pos );
		$_part2 = substr( $_query, $_pos +  strlen($_place_holder) );
		$_replacement = $_arr_args[$_i];
	
		if( is_numeric($_replacement) ){
			# leave	
		} else {
			$_replacement = "'" . addslashes($_replacement) . "'";
		}
		$_query =  $_part1 . $_replacement . $_part2;
	
		
		#vd("-$_part1-\n	-$_part2- ");

		$_i++;

	}


	#vd("orig $_query_orig");
	#vd($_query,"after");

	return execute_query($_query);
	
}






/**
		Updates or inserts a SINGLE record based on an ARRAY of key-values.
		Saves us the hassle of building insert and update strings.

		Takes an array with key-values,
		Takes the name of the primary key
		Takes the name of the table
		Assumes that the pk column is auto-increment.
		Therefore: If the given pk-Column has no value, we assume INSERT, otherwise UPDATE
		
		If param $_pk_col == "force_insert", we insert any way.
		This is handy, ihf there is NO autoincrement primary key and we wanna submit pk-values for the insert
		
		$_pk_col may be a scalar value OR an array of several primary key column names
		
		
*		building an insert and an update string and executing the insert/update.

		returns sql result

		TODO
		arr_options was added later on; 
				force_insert should be moved into arr_options; 
				the priv-Param should be extinguished
*		
*/
function insert_update($_arr, $_pk_col, $_table_name, $_arr_options = array() ){


	$_insert_ignore = '';			# either emtpy string or 'IGNORE'; enables insertion-or-updating without checking whether record exists; allows recklessly trying to insert first, and senselessly updating thereafter; see images_comment_async_update.php for application example
	$_log_suppress = false;
	extract( expand_options_array($_arr_options) );
	

	$link = connectPublicDB();			# open here above for mysql-real-escape-string
	
	if( ! $_pk_col  AND ! @sizeof($_pk_col) ){
		die("insert_update() braucht die/den Spaltennamen für den Primärschlüssel.");
	} else {
			
		if( is_array($_pk_col) AND @sizeof($_pk_col) ){
			$_arr_pk_cols = $_pk_col;				# array of primary key columns
		} else {
			$_arr_pk_cols = array( $_pk_col );	# single scalar primary key column
		}
	}
	
	#vd($_arr);
	#vd($_arr_pk_cols);

  $_u_string = "";		// update string
  $_i_string = "";		// insert string
  $_v_string = "";		// value string
  
  #  I am not sure, whether it is advisable to
  #		carry out this operation here 
  #		as it may already be done in the callee script
  #		however, it seems idempotent!
  $_arr = escape_for_sql($_arr);		# added 20110908
  
  foreach( $_arr as $_key => $_val ){
  	
 		if( in_array( $_key , $_arr_pk_cols)  ){
 			continue;					# do not update primary key	
 		}

		if( starts_with( trim($_val), "do_not_enclose" )	){
			# do NOT clad value into apostrophes 
			# instead remove the escaped apostrophes of escape for sql
			$_val = str_ireplace( chr(92).chr(39), chr(39), $_val);	#  \'   => '
			$_val = str_ireplace( "do_not_enclose","", $_val);	
		} else {
			$_val  =  "'$_val'";			
		}



		if( $_u_string  ) $_u_string = $_u_string . ", ";
		if( $_i_string  ) $_i_string = $_i_string . ", ";
		if( $_v_string  ) $_v_string = $_v_string . ", ";
		
		
		# prepend sumitted value to existing value of database column
		# only meaningful for string data of course
		# Note: also works for insert ;-)
		
		if( stristr($_val, "operation_prepend" )	){
			$_val = str_ireplace( "operation_prepend", "", $_val);
			$_u_string = $_u_string . "  $_key= concat_ws(' , $_val,$_key ) ";		# bp20100224: concat( NULL, ... ) returns null - to avoid this, we use concat_ws, which essentially ACCEPTS null arguments			

		} else if( starts_with($_val, "operation_append" )	){
			$_val = str_ireplace( "operation_append", "", $_val);
			$_u_string = $_u_string . "  $_key= concat_ws(' , $_key,$_val ) ";					

		} else {
			#echo "  $_key=$_val <br>";
			
			$_u_string = $_u_string . "  $_key= $_val ";
			
		}
		
		$_i_string = $_i_string . "  $_key  ";
		$_v_string = $_v_string . "  $_val  ";
 	
  }




	$_where_clause = " 1=1 ";
	$_clean_update_possible = true;		# init assumption
	
	
	foreach($_arr_pk_cols as $_key_unused => $_lp_pk_col){

		$_lp_pk_val = @$_arr[$_lp_pk_col];		
		if( isset($_lp_pk_val) ){
			# good	
		} else {
			$_clean_update_possible = false;
		}
		if( $_lp_pk_col == "force_insert" ){
			$_clean_update_possible = false;
		}

		if( starts_with( trim($_lp_pk_val), "do_not_enclose" )	){
			# do NOT clad value into apostrophes 
			# instead remove the escaped apostrophes of escape for sql
			$_lp_pk_val = str_ireplace( chr(92).chr(39), chr(39), $_lp_pk_val);	#  \'   => '
			$_lp_pk_val = str_ireplace( "do_not_enclose","", $_lp_pk_val);	
		} else {
			$_lp_pk_val  =  "'$_lp_pk_val'";			
		}

		$_where_clause .=  " \n AND $_lp_pk_col = $_lp_pk_val  ";
				
		
	}
	

	if( $_clean_update_possible ){
			$_operation = "Änderung (update)";
			$_q = "update $_table_name 
						set $_u_string
						where $_where_clause
			";
	}else{
			$_operation = "Einfügung (insert) ";
			$_q = "insert $_insert_ignore into $_table_name 
						( $_i_string )
						values ($_v_string)
			";
		
	}
	if( isset($_GET['dbg']) AND $_GET['dbg'] > 20 ) vd($_q);

	
	$_res3 = mysql_query($_q);

	# $_res3 should be == 1 if the query went through
	#	even if the where clause matched no records
	if( $_res3 ){
		$_info = mysql_info();
		if( $_info) $_info = ": $_info";
		if( ! $_log_suppress ){
			log_message( " $_operation in Tabelle $_table_name erfolgreich $_info" );		
		}


	}else{
		if( $_last_err = mysql_errno() ) print_error( __FUNCTION__ ."(): Datenbankfehler Nr. $_last_err; Meldung: " . mysql_error() );
		echo "mysql info says: " . mysql_info() . "<br>\n";
		echo "query was <pre> $_q </pre>\n";
	}
	
	handle_warnings($_arr_options);
	
	mysql_custom_close($link);

	return $_res3;

}











function MODEL_FUNCTIONS(){}




function insert_missing_translations($_key){

	$_test_is_number = floatval(kommata_zu_punkten($_single_word));
	if( abs($_test_is_number) > 0.001 ) return;

	$_arr_pk_missing = array('col_key');

	$_lp_arr['col_key']   = $_key;
	$_lp_arr['col_count'] = " do_not_enclose 0 ";

	insert_update($_lp_arr, array("force_insert"),"tbl_translation_missing", array("insert_ignore" => "IGNORE" ));

	$_lp_arr['col_count'] = " do_not_enclose (col_count+1) ";
	insert_update($_lp_arr, $_arr_pk_missing,"tbl_translation_missing");

}




	/*
	
		returns the newest record, unless option "admit_all_versions" is added
	
		id is the auto-increment id column
		
		ref_table+fk are indexed, to quickly retrieve all fields of
		a particular foreign record
		
		the actual primary key is
			ref_table, fk, fieldname, lang, last_changed 
			
			You can think of it like a multidimensional array:	

			Read: Tablename=>Table-PrimaryKey=>Fieldname=>Language=>ArrayIndex=>Version = value

			The version is designed as date-time column - and is rounded by hours or days
				thus keeping the total number of versions per hour or day limited.
			
			rules for new versions:
				if the last version differs less than 3 characters
				from the current version, we inherit previous approval

			if the last version does NOT differ, we do not add
				or even remove an existing (back-changed) version

				
			to keep the log of versions limited, 
				we clean up with each insert
				everything older than  1 hour  is reduced to one version per hour
				everything older than 24 hours is reduced to one version a day
				
								depending on whether explicit search criteria are set or not
								the subquery is either restricted by the explicit search criteria
									
									AND 	t.fieldname    = '{$_fieldname}'   
									AND 	t.subkey    	= '{$_subkey}'   
									AND 	t.lang      	= '{$_lang}'   		
								or	implicitly by the current outer table values
									AND 	t1.fieldname   = t.fieldname
									AND 	t1.subkey    	= t.subkey
									AND 	t1.lang      	= t.lang   		

								additional restrictions upon non-primary key columns
								must can be specified outside the max(version) clause
								or inside
								
								for instance: status_approval = 'approved'
									outside => all recent versions, who are approved (possibly NONE)
									inside  => the last version, that was approved (but may be no longer current)


			to keep the access fast and furious - the table is heavily indexed
			for access.
			
			This will be expensive upon inserts, updates and deletes
			
			The table is accessed with the InnoDB engine
			
	
	*/
function retrieve_multilang(
		  $_ref_table = 'tbl_images'
		, $_str_fk_ids
		, $_fieldname 
		, $_subkey 		= ''
		, $_lang			= '' 
		, $_admit_all_versions	 = false
		, $_additional_field_name	= ''
		, $_additional_field_value = ''
		, $_additional_inner_outer = 'outer'
){



	if( !$_ref_table OR ! $_str_fk_ids ){
		print_error("minimal parameters missing - -$_ref_table- -$_str_fk_ids-");	
		return;
	}

	$_str_fk_ids = trim($_str_fk_ids);
	if( $_str_fk_ids == "<all_rows>" ){
		$_constraint_fk = "";		
	} else {
		if( substr($_str_fk_ids,0,1) <> '(' ) $_str_fk_ids = "(".$_str_fk_ids;
		if( substr($_str_fk_ids,-1)  <> ')' ) $_str_fk_ids = $_str_fk_ids .")";		
		$_constraint_fk = " AND fk		IN   {$_str_fk_ids}	  ";
	}
	
	
	
	$_constraint_fieldname_outer = "";
	$_constraint_fieldname_inner = "AND 	t1.fieldname	 = t.fieldname ";
	if( $_fieldname ){
		$_constraint_fieldname_outer = "	AND 	t.fieldname	 = '{$_fieldname}' ";
		$_constraint_fieldname_inner = "	AND 	t1.fieldname	 = '{$_fieldname}' ";
	}
	$_constraint_subkey_outer = "";
	$_constraint_subkey_inner = "AND 	t1.subkey	 = t.subkey ";
	if( $_subkey ){
		$_constraint_subkey_outer = "	AND 	t.subkey	 = '{$_subkey}' ";
		$_constraint_subkey_inner = "	AND 	t1.subkey	 = '{$_subkey}' ";
	}
	$_constraint_lang_outer = "";
	$_constraint_lang_inner = "AND 	t1.lang	 = t.lang ";
	if( $_lang ){
		$_constraint_lang_outer = "	AND 	t.lang	 = '{$_lang}' ";
		$_constraint_lang_inner = "	AND 	t1.lang	 = '{$_lang}' ";
	}


	$_clause_admit_all_versions = "";
	if( $_admit_all_versions ){
		$_clause_admit_all_versions = " 1 OR ";	
	}


								
	
	$_constraint_additional_outer = "";
	$_constraint_additional_inner = "";
	if( $_additional_field_name AND $_additional_field_value ){

		if( $_additional_inner_outer == 'outer' ){
			$_constraint_additional_outer = "		AND 	t.{$_additional_field_name}	 = '{$_additional_field_value}' ";
			
		} else {
			$_constraint_additional_inner = "		AND 	t1.{$_additional_field_name}	 = '{$_additional_field_value}' ";			
		}
	}



	$_q1 = "
		select * 
		from tbl_multi_lang_string t
		where		 1=1	
				AND ref_table = '{$_ref_table}'	 
				$_constraint_fk 
				$_constraint_fieldname_outer
				$_constraint_subkey_outer
				$_constraint_lang_outer
				$_constraint_additional_outer

				AND ( 
					$_clause_admit_all_versions
					
					last_changed = 
						(
							select max(t1.last_changed)
							from	tbl_multi_lang_string t1
							where 		t1.ref_table 	= '{$_ref_table}'
									AND	t1.fk		 	=  t.fk
									$_constraint_fieldname_inner
									$_constraint_subkey_inner
									$_constraint_lang_inner
									$_constraint_additional_inner
						)
					
					
				)
				
			order by fk, (fk+0), fieldname, lang, subkey, last_changed desc
	";	

	$_arr_ret = execute_query($_q1);

	#vd($_q1);	
	#vd($_arr_ret);
	
	return $_arr_ret;
}


function retrieve_multilang_last_approved(
		  $_ref_table = 'tbl_images'
		, $_str_fk_ids
		, $_fieldname 
		, $_subkey 		= ''
		, $_lang			= '' 
){

	return retrieve_multilang(
			  $_ref_table 
			, $_str_fk_ids
			, $_fieldname 
			, $_subkey 
			, $_lang			
			, false			# admit all versions
			, 'status_approval'		# 'status_approval' additional col
			, 'approved'				# 'approved' additional val
			, 'inner'			#	OUTER
	);


}

function TREE_FUNCTIONS(){}

/*
	recursively retrieves all descendent elements 
	
	sorted by level

	parent-children relations are stored in a adjacency list manner.
	There are database specific extensions (Oracle's connect by) to
	retrieve such structures in one database request.
	
	There are also mysql stored-prodecure solutions to 
	retrieve tree data with one network request only.
	
	But most providers PROHIBIT mysql stored procedures.
	Therefore we use a "primitive" implementation, working with
	any relatational database, and pay the price of doing
	ONE database-network request per LEVEL.
	this approach delivers the nodes in a ugly level-wise structure,
	which we have to restructure :(


*/
function get_descendent_ids_levelwise($_mixed_from_ids, $_cur_level = 0,  $_arr_recursed_cumulative_element_hierarchy = array() ){

	$_max_level = 10;
	if( $_cur_level > $_max_level ) return $_arr_recursed_cumulative_element_hierarchy;

	global $_hierarchify_operator;

	
	$_arr_from_ids = standardize_to_array($_mixed_from_ids);
	if( sizeof( $_arr_from_ids ) < 1 ) return $_arr_recursed_cumulative_element_hierarchy;
	
	$_str_from_ids = implode($_arr_from_ids,",");
	
	if( $_cur_level == 0 ){
		foreach( $_arr_from_ids as $_key_unused => $_lp_from_id ){
			$_arr_this_level["{$_lp_from_id}"] = 
				array(	  'col_from'=> '0'
							, 'col_to' => $_lp_from_id
							, 'level'  => $_cur_level
				);
		}		
	} else {
		$_arr_this_level = array();	
	}
	
	$_cur_level++;

	
	$_arr_next_level = execute_query("
		select 	col_from
				,	col_to
				,  '$_cur_level' level
		from tbl_relation
		where 		col_from IN  ( $_str_from_ids )
				AND	relation_type = 'is_heading_to'
	
	");

	$_arr_ids_next_level = array_reorganize_by_subkey(
		$_arr_next_level
		, "col_to"
		, array("flatten") 
	);	
	#vd($_arr_ids_next_level);
	
	$_arr_next_level = array_reorganize_by_subkey(
		  $_arr_next_level
		, array("col_to")
		#, array("flatten") 
	);
	#vd($_arr_this_level);
	#vd($_arr_next_level);



	$_arr_recursed_cumulative_element_hierarchy 
		= 		$_arr_recursed_cumulative_element_hierarchy 
			+ 	$_arr_this_level 
			+ 	$_arr_next_level;
	#vd($_arr_recursed_cumulative_element_hierarchy);

	return get_descendent_ids_levelwise($_arr_ids_next_level, $_cur_level,  $_arr_recursed_cumulative_element_hierarchy); 
	
}



/*
	as we retrieve the descendents level-wise from the database
	to minimize database roundtrips,
	we now have to connect the elements into a tree
	as we want to display the tree in a linear fashion, we want a 
		linearized tree like in file system directory tree 
		- with all directories expanded
	
	first, we construct a meta-structure, 
	an array linking parent-with-children
	
	then we recurse from root node(s) and append the chilren
	into a linear manner

	While appending the children, they are also being sorted.
	
	this is accomplished by a once-for-all-levels call to 
	fct_descendents_sorting
		

*/
function make_linear_tree($_arr_did_sorted_levelwise){

	global $_arr_did_by_from_to;
	global $_arr_linear_tree;
	global $_ARR_SORTINDEX_BY_ID;
	$_arr_did_by_from_to		= array();
	$_arr_linear_tree			= array();
	$_ARR_SORTINDEX_BY_ID   = array();


	$_arr_did_by_level = array_reorganize_by_subkey(
		$_arr_did_sorted_levelwise
		, array("level","col_to")
	);
	$_arr_did_by_level = hierarchify_array($_arr_did_by_level);


	$_arr_did_by_from_to = array_reorganize_by_subkey(
		$_arr_did_sorted_levelwise
		, array("col_from","col_to")
	);
	$_arr_did_by_from_to = hierarchify_array($_arr_did_by_from_to);

	$_ARR_SORTINDEX_BY_ID = retrieve_sorting_array_for_ids($_arr_did_sorted_levelwise);
	
	#vd($_arr_did_by_from_to);
	#vd($_arr_did_sorted_levelwise);
	#vd($_arr_did_by_level);
	#vd($_ARR_SORTINDEX_BY_ID, "arr_sortindex_by_id");


	/* iterating root nodes - recursively append children ... */	
	$_root_nodes = $_arr_did_by_level[0];
	uksort($_root_nodes, "fct_descendents_sorting");
	foreach($_root_nodes as $_lp_to => $_arr_lp_root_node){
		$_arr_linear_tree[ $_arr_lp_root_node['col_to'] ] = $_arr_lp_root_node;
		make_linear_tree_append_descendents($_arr_lp_root_node);
	}
	
	#vd($_arr_linear_tree);
	
	return $_arr_linear_tree;
	
}

/*
	recursive function
	source and destination array shared as global variables
	for PERFORMANCE reasons
*/
function make_linear_tree_append_descendents($_node){
	
	#vd($_node, "lin_tree_rec");

	global $_arr_did_by_from_to;
	global $_arr_linear_tree;
	global $_ARR_SORTINDEX_BY_ID;
	static $_recursion_break;
	
	if( isset($_recursion_break) ) $_recursion_break++;
	else $_recursion_break=0;
	if( $_recursion_break > 100 ) return;
	

	#vd($_ARR_SORTINDEX_BY_ID,"arr_sortindex_by_id_rec");

	$_to   = $_node['col_to'];
	
	$_nodes_descendents = $_arr_did_by_from_to[$_to];
	
	if( is_array($_nodes_descendents) ){
		#vd($_nodes_descendents,"descendents_rec");
		uksort($_nodes_descendents, "fct_descendents_sorting");
		if( sizeof( $_nodes_descendents ) ){
			
			reset($_nodes_descendents);	# deprecated as Call-time pass-by-reference
			$_key_first = key($_nodes_descendents);
			$_nodes_descendents[$_key_first]['first'] = true;
			

			end($_nodes_descendents);		# deprecated as Call-time pass-by-reference
			$_key_last = key($_nodes_descendents);
			$_nodes_descendents[$_key_last]['last'] = true;
		}
		#vd($_nodes_descendents,"nodes_descendents_rec after sorting");
		
		
		foreach($_nodes_descendents as $_lp_to => $_lp_node_desc){
			$_arr_linear_tree[ $_lp_node_desc['col_to'] ] = $_lp_node_desc;
			if( $_to == $_node['col_from'] ) {
				# prevent eternal recursion
			} else {
				make_linear_tree_append_descendents($_lp_node_desc);				
			}
		}
	
	}
	
}


/*
	sequence/sort-order info is saved as adjacency list
	thus we have several sequences in a fractured manner
	as we iterate we, we encounter different sequences 
		at different positions
		
	we hit any sequence somewhere in between,
		we follow it all the way up and all the way down
		and marking the followed elements as already_sequenced
	

*/
function retrieve_sorting_array_for_ids($_arr_did_sorted_levelwise){
	
	# the ids may come as key-value array - or as key => array( col_to, col_from ...)
	if( 			is_array($_arr_did_sorted_levelwise) 
			AND 	sizeof($_arr_did_sorted_levelwise) 
	){
		$_first_elem = reset($_arr_did_sorted_levelwise);	# deprecated as Call-time pass-by-reference
		if( is_array( $_first_elem ) ){
			# retrieve sorting info
			$_arr_element_ids = array_reorganize_by_subkey(
				$_arr_did_sorted_levelwise
				, "col_to"
				, array("flatten") 
			);		
		} else {
			$_arr_element_ids = $_arr_did_sorted_levelwise;
		}
	} else {
		return array();	
	}

	#vd($_arr_element_ids,"arr_element_ids");

	$_str_from_ids = implode($_arr_element_ids,",");


	$_arr_sorting_raw = execute_query("
		select 	col_from
				,	col_to
				,  id
		from tbl_relation
		where 		col_from IN  ( $_str_from_ids )
				AND	relation_type = 'is_precursor_of'
	
	");

	$_arr_sorting_raw = array_reorganize_by_subkey(
		$_arr_sorting_raw
		, array("col_from")
	);



	# vd($_arr_sorting_raw, "sorting raw");
	if( 
					sizeof($_arr_did_sorted_levelwise) == 1
			AND	sizeof($_arr_sorting_raw) == 0 
	){
		# special case - SINGLE element per level has NO sorting info
		#return array_flip($_arr_did_sorted_levelwise);
		return array_flip($_arr_element_ids);
		
	}
	
	# init some helper arrays
	foreach($_arr_sorting_raw as $_from => $_arr_lp){
		$_to = $_arr_lp['col_to'];
		$_from_by_to[$_to]   = $_from;
		$_to_by_from[$_from] = $_to;
		$_a_already_sequenced[$_to]   = false;
		$_a_already_sequenced[$_from] = false;
	}


	#vd($_from_by_to,"from_by_to");
	#vd($_to_by_from,"to_by_from");
	#vd($_a_already_sequenced, "already sequenced");
	
	
	if( is_array($_to_by_from) ){
		foreach($_to_by_from as $_from => $_to){
			
			if( $_a_already_sequenced[$_from]  ){
				continue;
			} 
			$_a_already_sequenced[$_from] = true;		
			
			$_arr_fk_sortindex[$_from] = 0;

			
			# pursue all the way up
			$_up_counter = 1;
			$_up_previous = $_from;
			while( isset( $_to_by_from[$_up_previous] ) ){

				#vd(" pursuing up from $_up_previous to $_to_by_from[$_up_previous]; ");
				$_up_previous = $_to_by_from[$_up_previous];
				$_arr_fk_sortindex[$_up_previous] = $_up_counter;
				if( $_a_already_sequenced[$_up_previous]  ){
					print_error("recursive sequence up: $_up_previous");
					break;	# prevent circulars within single sequence
				} 
				$_a_already_sequenced[$_up_previous] = true;
				$_up_counter++;
			}
			
			# pursue all the way down
			$_down_counter = -1;
			$_down_previous = $_from;
			while( isset( $_from_by_to[$_down_previous] ) ){
				#vd(" pursuing down from $_down_previous to $_from_by_to[$_down_previous]; ");
				$_down_previous = $_from_by_to[$_down_previous];
				$_arr_fk_sortindex[$_down_previous] = $_down_counter;
				if( $_a_already_sequenced[$_down_previous]  ) {
					print_error("recursive sequence down: $_down_previous");
					break;	# prevent circulars within single sequence	
				}
				$_a_already_sequenced[$_down_previous] = true;
				$_down_counter--;
			}
			
		}		


	} else {
		# no array - by from
		return array();
	}			


	#vd($_a_already_sequenced," sequenced from database");


	

	# restrict to input
	# =================
	$_arr_input_ids = array_flip($_arr_element_ids);
	foreach($_arr_fk_sortindex as $_lp_fk => $_sort_index){
		if( isset($_arr_input_ids[$_lp_fk]) ){
			$_arr_fk_sortindex_cleansed[$_lp_fk] = $_sort_index;
		}
	}	
	#vd($_arr_fk_sortindex_cleansed, "cleansed now sorted as ");
	
	return $_arr_fk_sortindex_cleansed;
	
}



function fct_descendents_sorting($a, $b){

	global $_ARR_SORTINDEX_BY_ID;


	$_sort_a = $_ARR_SORTINDEX_BY_ID[$a];
	$_sort_b = $_ARR_SORTINDEX_BY_ID[$b];


	if( 
					! isset($_ARR_SORTINDEX_BY_ID[$a])
			OR		! isset($_ARR_SORTINDEX_BY_ID[$b])
	
	){
		if( get_uri('async') ){ 
			# do not output error append	
		} else {
			print_error("
				insufficient sort array info: <br>
				$a --- $b (fk)<br>
				$_sort_a --- $_sort_b (sortindex)<br>
				make_sequence(a,b) ($a,$b);
			");
		}
		# repair
		make_sequence($a,$b);
		return  -1;
		
	}
	
	
	
	if( $_sort_a < $_sort_b ) return -1;
	if( $_sort_a > $_sort_b ) return  1;


}




function get_successor($_source_fk){

	$_arr_succ = execute_query_get_first("
		select col_to, id
		from tbl_relation
		where 		col_from   		= $_source_fk  
				AND	relation_type	= 'is_precursor_of'
	");

	if( isset( $_arr_succ['col_to']) ){
		$_ret = $_arr_succ['col_to'];
	} else {
		$_ret = -1;
	}

	return $_ret;
	
}


function get_predecessor($_source_fk){

	$_arr_pred = execute_query_get_first("
		select col_from, id
		from tbl_relation
		where 		col_to   		= $_source_fk  
				AND	relation_type	= 'is_precursor_of'
	");

	if( isset( $_arr_pred['col_from']) ){
		$_ret = $_arr_pred['col_from'];
	} else {
		$_ret = -1;
	}

	return $_ret;
	
}

function get_parent($_source_fk){

	$_arr_pred = execute_query_get_first("
		select col_from, id
		from tbl_relation
		where 		col_to   		= $_source_fk  
				AND	relation_type	= 'is_heading_to'
	");
	if( isset( $_arr_pred['col_from']) ){
		$_ret = $_arr_pred['col_from'];
	} else {
		$_ret = -1;
	}
	
	return $_ret ;
	
}

function get_children($_source_fk){

	if( ! ($_source_fk>-1) ) {
		return array();		
	}

	$_arr_old_children = execute_query("
		select col_to fk
		from tbl_relation
		where 		col_from   		= $_source_fk  
				AND	relation_type	= 'is_heading_to'
	","fk");
	
	
	$_arr_old_children = array_reorganize_by_subkey(
		$_arr_old_children
		, "fk"
		, "flatten"
	);	

	
	if( is_array($_arr_old_children) AND sizeof($_arr_old_children) ){
		return $_arr_old_children;
	} else {
		return array();
	}
	
	
}

/*
	attention - returns children keys
	in array as KEYS

	get the last child for instance with 
		end(&$_arr_children_sortindex);
		$_predecessor_last_child = key( $_arr_children_sortindex );

*/
function get_children_sorted($_sk){

	$_arr_pred_children = get_children($_sk);
	if( sizeof($_arr_pred_children) ){
		$_arr_children_sortindex = retrieve_sorting_array_for_ids($_arr_pred_children);
		#vd($_arr_pred_children);
		#vd($_arr_children_sortindex);

		asort($_arr_children_sortindex);
		return $_arr_children_sortindex;	
	} else {
		return array();	
	}

	
}

function make_sequence($_pred=0,$_succ=0){

	
	global $_arr_pk_relation;

	$_before_root = 0; # -1


	$_arr_upd['relation_type']   = 'is_precursor_of';


	if( 			($_pred > $_before_root)
			AND 	($_succ > $_before_root)
	){
		
		vd1("tied sequence together $_pred - $_succ");
		execute_query("
			delete 
			from tbl_relation
			where 	relation_type   = 'is_precursor_of'
				AND	(			
										col_from IN ($_pred)
							OR			col_to 	IN ($_succ)
									/* exchange order */
							OR		(	col_from= $_succ AND col_to = $_pred  ) 
						)
		");


		$_arr_upd['col_from'] = $_pred;
		$_arr_upd['col_to']   = $_succ;
		insert_update($_arr_upd, array("force_insert") ,"tbl_relation");		
		#insert_update($_arr_upd, $_arr_pk_relation     ,"tbl_relation");		

	} else if ( $_pred > $_before_root ){
		
		vd1("but no successor => cut from last element
				predecessor $_pred becomes last item of sequence, no successor ");
		execute_query("
			delete 
			from tbl_relation
			where 	relation_type   = 'is_precursor_of'
				AND	col_from IN ($_pred)
		");
		
	} else if ( $_succ > $_before_root ){
		
		vd1("but no predessor => cut from first element
				successor $_succ becomes first  item of sequence, no predessor");
		execute_query("
			delete 
			from tbl_relation
			where 	relation_type   = 'is_precursor_of'
				AND	col_to IN ($_succ)
		");
		
	} else {
		# single item under a heading was cut, no sequence change needed
	}


	
}

function make_parent_child($_parent,$_child){

	global $_arr_pk_relation;

	if( $_parent == -1 ) return;
	
	if( $_child < 1 ){
		#print_error("possible is only 0-4 or 0-15 but not 15-0");
		return;
	}

	execute_query("
		delete 
		from tbl_relation
		where 	relation_type   = 'is_heading_to'
			AND	col_to 	IN ($_child)
	");


	$_arr_upd['relation_type']   = 'is_heading_to';
	$_arr_upd['col_from'] = $_parent;
	$_arr_upd['col_to']   = $_child;

	insert_update($_arr_upd, array("force_insert") ,"tbl_relation");		


}


function repair_root_nodes(){




	# repair self-referential parent-child records
	$_arr_self_ref = execute_query("
		select id, col_to
			from tbl_relation 
			where  
						relation_type = 'is_heading_to' 
				and	col_from = col_to	
	", "id");
	foreach($_arr_self_ref as $_key_id => $_arr_lp){
		vd("repairing self referential record - $_arr_lp[col_to] becomes root node");
		execute_query("
			update tbl_relation 
			set col_from = 0 
			where id = '$_key_id'
		");
	}
	




	# bring back orphaned elements als root nodes
	# make all orphaned elements root nodes
	$_arr_orphaned_elements = execute_query("
			select distinct fk 
			from tbl_multi_lang_string 
			where 
				ref_table='tbl_article'	
				and fk not in 
		(
			select col_to
			from tbl_relation 
			where  
						relation_type = 'is_heading_to' 
				/*
				and		col_from = 0 
				*/
		)
		order by fk+0
	","fk");
	$_arr_orphaned_elements = array_keys($_arr_orphaned_elements);
	foreach($_arr_orphaned_elements as $_key_unused => $_lp_fk){
		make_parent_child(0,$_lp_fk);
		vd("bringing back orphaned element fk $_lp_fk as root node");
	}
	

	
	# thereby sequencing unsequenced root nodes
#	$_arr_root_ids = get_children_sorted(0);		




	##  repair double sequences
	$_arr_double_seq = execute_query("
		select 	col_from
				,  group_concat(col_to) col_to_s
				,  count(*) anz
		from tbl_relation
		where 		relation_type = 'is_precursor_of'
		group by col_from
		having count(*) > 1
	");
	
	if( is_array($_arr_double_seq) ){
		foreach($_arr_double_seq as $_key_unused => $_arr_lp){
			vd($_arr_lp, "repairing sequence for ");
			$_arr_succ = explode( ",", $_arr_lp['col_to_s'] );
			$_arr_succ = rsort($_arr_succ);
			$_first_succ = $_arr_succ[0];
			make_sequence($_arr_lp['col_from'],$_first_succ);
		}
	}


}



function handle_warnings($_arr_options=array()){

	extract( expand_options_array($_arr_options) );
	

	if( is_dev_or_test() OR get_param('dbg_show_mysql_warnings')  ) {

		# http://stackoverflow.com/questions/47589/can-i-detect-and-handle-mysql-warnings-with-php
		$warningCountResult = mysql_query("SELECT @@warning_count");
		if( $warningCountResult ){
			$warningCount = mysql_fetch_row( $warningCountResult );
			if( $warningCount[0] > 0 ){
	
				//Has warnings
				$warningDetailResult = mysql_query("SHOW WARNINGS");
				if(  $warningDetailResult ){
					while(  $warning = mysql_fetch_assoc( $warningDetailResult ) ){
						if( 
										$warning['Level'] == 'Note' 
								AND	$warning['Code']  == '1050' 
						){
							echo "$warning[Message]<br>";
							continue;
						}
						if( 		
								$warning['Code']  == $_skip_warning
						){
							#echo "skipping $warning[Message]<br>";
							continue;
						}

						print_error(" query produced a warning(s): ");
						vd( $warning);
						vd( $_q );
					}					
					
				} 
	
	
			}//Else no warnings
		}

	}

	
	
}






#	usage:
#		compare_array_by_key(1,1,'similarity');
#		uasort( $_arr_all["others"] , "compare_array_by_key" );
function compare_array_by_key($a, $b, $_arg_key=''){

	static $_key;	
	if( $_arg_key )$_key = $_arg_key;
	if( ! isset($_key) ) print_error("function must be initialized with a compare key first");
	
	if( !isset($a[$_key]) OR  !isset($b[$_key]) ) return 0;
	
	$_ret = 0;
	$a[$_key] >= $b[$_key] ? $_ret=1 : $_ret=-1 ;
	return $_ret;
}



/*
returns text limited by a specified length of characters but keeping words intact. the final character count will not be exact since it is affected by the possible removing of the a long word or by the addition of the ellipsis.
paramaters:

	string - the input string
	chars - the length of characters wanted
	elli - the ellipsis to be used, defaults to '...'
*/


function shorten($_arg='', $chars=40, $_ellipse='...'){
	$_string_wrapped_words = wordwrap($_arg, $chars, "temporal_delimiter", false);  
	$_arr_wrapped_words	= explode("temporal_delimiter", $_string_wrapped_words);
	$_first_line  = $_arr_wrapped_words[0];
	$_second_line = $_arr_wrapped_words[1];
	return ( $_second_line ) ? $_first_line.$_ellipse : $_first_line;
}


function array_to_table($a, $_arr_options=array() ){

	$_format_dates = false;
	$_suppress_empty = false;
	$_heading = '';
	$_cutoff = 0;
	$_popup_columns = array()			# make val_col to appear as popup over key_col, remove val_col
	extract( expand_options_array($_arr_options) );
	

	$_cols_mouseover_src = array_flip($_popup_columns);

	vd( $_arr_options );
	vd( $_cols_mouseover_src );
	
	if( is_array($a) AND (sizeof($a) > 0) 
	){

	   # test if array is only single row; 
	   #	artificially blow it up
	   #  does not work yet
	   if( ! is_array( $a[0] ) ){
	   	$_wanton_key = 0;
			foreach( $a as $_key => $_skalar_val ){
				$a[$_key] = array( "col1".$_wanton_key => $_skalar_val);
				$_wanton_key++;
			}
	   }

		#continue	


	} else {
		if( $_suppress_empty ){
			return "";			
		} else {
			$t = "Momentan keine hierfür Daten vorhanden.<br><br>";
		  	if( $_heading )  $t = "<h2>$_heading</h2>{$t}";
			return $t;			
		}
	}
	
	$_cnt_cols = sizeof( array_keys($a[0]) );
	$_cnt_cols -= sizeof( $_cols_mouseover_src );
	
	$t = '';
	$t.= "
		<style>
			table {
				border: 1px #bbb solid;
				padding: 0px;
				margin:  0px;
				
			}
			td, th {
				padding: 6px 4px;
				padding-left: 6px;
				margin:  0px;
			}

			  td.class_not_first
			, th.class_not_first
			 {
				border-left: 1px #bbb solid;
			}
			th
			 {
				border-bottom: 1px #bbb solid;
			}
		</style>
	
	
	
	";
	


   $t.="<table>";

   # header
   $t.="<tr>";
   $_class_not_first = "";
	foreach( $a[0] as $_col_name => $_val_unused ){
		if( $_cols_mouseover_src[$_col_name] ) {  continue; }
		$t.=	"<th $_class_not_first >".$_col_name."</th>";		
		$_class_not_first = " class='class_not_first' ";
	}
   $t.="</tr>";

	# body
	$_cntr_row = 0;
   foreach($a as $row){
		$_cntr_row++;
		if( $_cutoff AND $_cntr_row > $_cutoff ){
			$t.=	"<tr><td $_class_not_first colspan='{$_cnt_cols}' style='width:100%;text-align:center;'>   ... </td></tr>";		
			break;
		}

	   $_class_not_first = "";
	   $t.="<tr>";
		foreach( $row as $_col_name => $_val ){
			if($_format_dates ){
				$_try_it = render_mysql_date($_val);
				if(  $_try_it <> '0000-00-00' )	{
					$_val = $_try_it;
				}
			}
			
			$t.=	"<td $_class_not_first >{$_val}</td>";		
			$_class_not_first = " class='class_not_first' ";
		}
	   $t.="</tr>";


   }
   $t.="</table>";
   
 	#$t = str_ireplace("<","\n<",$t );
  
  	if( $_heading )  $t = "<h2>$_heading</h2>{$t}";
   
   return $t;
}

function TEXT_DIFF_RENDERING(){}
/*
	Paul's Simple Diff Algorithm v 0.1
	(C) Paul Butler 2007 <http://www.paulbutler.org/>
	May be used and distributed under the zlib/libpng license.
	
	This code is intended for learning purposes; it was written with short
	code taking priority over performance. It could be used in a practical
	application, but there are a few ways it could be optimized.
	
	Given two arrays, the function compute_string_diff will return an array of the changes.
	I won't describe the format of the array, but it will be obvious
	if you use print_r() on the result of a compute_string_diff on some test data.
	
	render_string_diff is a wrapper for the compute_string_diff command, it takes two strings and
	returns the differences in HTML. The tags used are <ins> and <del>,
	which can easily be styled with CSS.  
*/

function compute_string_diff($old, $new){
	$maxlen = 0;
	foreach($old as $oindex => $ovalue){
		$nkeys = array_keys($new, $ovalue);
		foreach($nkeys as $nindex){
			$matrix[$oindex][$nindex] = isset($matrix[$oindex - 1][$nindex - 1]) ?
				$matrix[$oindex - 1][$nindex - 1] + 1 : 1;
			if($matrix[$oindex][$nindex] > $maxlen){
				$maxlen = $matrix[$oindex][$nindex];
				$omax = $oindex + 1 - $maxlen;
				$nmax = $nindex + 1 - $maxlen;
			}
		}	
	}
	if($maxlen == 0) return array(array('d'=>$old, 'i'=>$new));
	return array_merge(
		compute_string_diff(array_slice($old, 0, $omax), array_slice($new, 0, $nmax)),
		array_slice($new, $nmax, $maxlen),
		compute_string_diff(array_slice($old, $omax + $maxlen), array_slice($new, $nmax + $maxlen)));
}

function render_string_diff($old, $new){
	$ret = '';
	$diff = compute_string_diff(explode(' ', $old), explode(' ', $new));
	foreach($diff as $k){
		if(is_array($k))
			$ret .= (!empty($k['d'])?"<del>".implode(' ',$k['d'])."</del> ":'').
				(!empty($k['i'])?"<ins>".implode(' ',$k['i'])."</ins> ":'');
		else $ret .= $k . ' ';
	}
	return $ret;
}



/*
	for instance '/vermittler' ...
	but without get-params
*/
function get_uri($_compare_to = '/' ){
	
	$_uri = $_SERVER['SCRIPT_NAME'];		# bspw. http://www.yoursite.com/example/index.php --> /example/index.php	 see http://php.about.com/od/learnphp/qt/_SERVER_PHP.htm
	if( starts_with($_uri, $_compare_to ) ){
		return $_uri;
	} else {
		return false;	
	}
	
}


/*
	this is for our javascript "templating engine" - YESSS
	
	1.) we construct templates using the the ususual functions: 
				render_input(); render_text(); render_inline_block()
	2.) we wrap the entire structure 
			with the a ID attribute of the same name as the template
			with the a css CLASS	of the same name as the template
			
			the ID can be used to for jQuery-selection and changed to whatever wished for for later identification

	3.) we include placeholders <span> Tags mit id='fieldname'
	4.) we put the result into a PHP variable 
	5.) now we fill this PHP string into a javascript variable and make it globally available
		 	we have to deal with quotes and double quotes and new lines
	6.) finally we append it into the DOM and fill it up based on its ID

			$('#sortbl02 div#file_upload_info_widget').attr('id',file.id);

			$.each(map_file_data, function(key, value ){ 
				$('#sortbl02 div#'+file.id+' #'+key).empty();
				$('#sortbl02 div#'+file.id+' #'+key).append(value);
			});

		 	
	
	
	7.) Alternative: we just bring the code into one line as input 
			for json-encoding
			then set $_js_var_name="make_json_string"
	
*/
function prepare_for_js_or_json($_template, $_js_var_name ){

	$_str_ret = "";
	$_arr_json = array();
	
	$_str_ret = "	var $_js_var_name = \"\"; \n";

	$_template	= str_ireplace("\r\n","\n",$_template);

	$_arr_template = explode("\n", $_template);
	foreach($_arr_template as $_key_unused => $_line ){
		$_line = str_replace('"','\"',$_line);
		$_str_ret .= "	$_js_var_name += \"$_line\"; \n";
	}
	
	if( $_js_var_name == "make_json_string" ){
		$_template	= str_ireplace("\n"," ",$_template);
		#$_template	= str_replace('"','\"',$_template);		# not neccessary
		return $_template;
		
	} else {
		$_str_ret = "
			<script>
				// generated by ".__FUNCTION__."
				$_str_ret
				//$('body').append('<p style=text-align:left >this is template $_js_var_name</p>');
				//$('body').append($_js_var_name);
			</script>
		";		
	}
	
	
	
	return $_str_ret;
}

function cleanse_param($_str, $_with = ''){
	# we do not allow the hyphen - 
	$_str = preg_replace('/[^a-z0-9äöüß,-_\s\.]+?/i',$_with,$_str);
	
	
	#	as sql comments could be injected with it: username = 'admin' -- 
	$_str = str_replace("--","",$_str);

	return $_str;
}


	function standardize_all_newlines($_str){
		$_arr = array(
			  "\r\n"
			, "\n\r"
			, "\n"
			, "\r"
		);
		$_str = str_ireplace( $_arr,"\n",  $_str );
		return $_str;
	}

	function cleanse_double_newlines($_str){
		$_arr = array(
			  "\n\n\n\n"
			, "\n\n\n"
			, "\n\n"
			, "\n"
		);
		$_str = str_ireplace( $_arr,"\n",  $_str );

		$_str = replace_all_whitespace_with( $_str, " ");
		$_str = str_ireplace( "\n \n","\n",  $_str );
		
		return $_str;
	}


# expand umlaute
function expand_umlauts($_str){
	
	$_arr_repl = array(
		 'ß' => 'ss'
		,'Ä' => 'Ae'
		,'Ö' => 'Oe'
		,'Ü' => 'Ue'
		,'ä' => 'ae'
		,'ö' => 'oe'
		,'ü' => 'ue'
	
	);
	return str_replace( array_keys($_arr_repl),array_values($_arr_repl),$_str);
}



# cleanse out all unusual characters from a message for instance.
# we permit only Buchstaben, Ziffern, Leerzeichen and Satzzeichen.
function replace_non_ascii($_str, $_with = '', $_condense = false){
	$_str_ret = preg_replace('/[^a-z0-9äöüß]+?/i',$_with,$_str);		# ? makes non greedy
	if( $_condense ) $_str_ret = condense_char($_str_ret,$_with);
	return $_str_ret;
}



function replace_non_ascii_satzzeichen($_str, $_with = ''){
	return preg_replace('/[^a-z0-9äöüß,_!\s\-\.]+?/i',$_with,$_str);	# ? makes non greedy
}

function condense_char($_str, $_char){

	if( strlen($_str)  <  1 ) return $_str;
	if( strlen($_char) <> 1 ) return $_str;

	$_str = preg_replace('/['.$_char.']+/i',$_char,$_str);

	if( substr($_str,0,1) == $_char  ) $_str = substr($_str,1);
	if( substr($_str,-1 ) == $_char  ) $_str = substr($_str,0,-1);


	return $_str;

}


function replace_annoying_utf8_chars($_str, $_with = ''){
	
	# remove annoying characters
	$_chars = array(
	     '\xc2\x82' => ','        # High code comma
	    ,'\xc2\x84' => ',,'       # High code double comma
	    ,'\xc2\x85' => '...'      # Tripple dot
	    ,'\xc2\x88' => '^'        # High carat
	    ,'\xc2\x91' => '\x27'     # Forward single quote
	    ,'\xc2\x92' => '\x27'     # Reverse single quote
	    ,'\xc2\x93' => '\x22'     # Forward double quote
	    ,'\xc2\x94' => '\x22'     # Reverse double quote
	    ,'\xc2\x95' => ' '
	    ,'\xc2\x96' => '-'        # High hyphen
	    ,'\xc2\x97' => '--'       # Double hyphen
	    ,'\xc2\x99' => ' '
	    ,'\xc2\xa0' => ' '
	    ,'\xc2\xa6' => '|'        # Split vertical bar
	    ,'\xc2\xab' => '<<'       # Double less than
	    ,'\xc2\xbb' => '>>'       # Double greater than
	    ,'\xc2\xbc' => '1/4'      # one quarter
	    ,'\xc2\xbd' => '1/2'      # one half
	    ,'\xc2\xbe' => '3/4'      # three quarters
	    ,'\xca\xbf' => '\x27'     # c-single quote
	    ,'\xcc\xa8' => ''         # modifier - under curve
	    ,'\xcc\xb1' => ''          # modifier - under line
	);
	
	return str_ireplace( array_keys($_chars),$_chars, $_str );
	
	
}

# including tabs AND newlines - default leads to replace with nothing
function replace_all_whitespace_with($_arg_str, $_with = '' ){

	#$_str_ret = preg_replace("/[\s]*/",'$1',$_arg_str);		 # makes every character a group - difficult for replacements
	#$_str_ret = preg_replace("/\s\s+/",$_with,$_arg_str);	# would not replace single tab or single newlines


	$_str_ret = $_arg_str;
	$_str_ret = preg_replace("/\s/"	 ,' ' ,$_str_ret);	# normalize all whitespace to space
	$_str_ret = preg_replace("/\s\s+/",' ' ,$_str_ret);	# now condense double spaces into one space
	$_str_ret = str_ireplace(" ",$_with,$_str_ret);			# now replace

	return $_str_ret;	
}


#	takes email address and telnr out of a comment
function replace_emails_or_phone_numbers( $comment ){
	if( strlen($comment) < 1 ){
		return "";
	}
	$comment=preg_replace(	 "/[a-z0-9\._-]+@+[a-z0-9\._-]+\.+[a-z]{2,3}/i" , "[email]", $comment);
	$comment=preg_replace(	 "/[0-9\/_-]{3,}/i" ,"[telnr]", $comment);
	return $comment;
}



function get_status_icon( $_status , $_arr_options = array() ){
	
	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 

	$_add_title = "";
	$_height	= 30;
	$_id		= "";
	extract( expand_options_array($_arr_options) );

	if( ! $_status ) $_status = "new";

	$_add_style = "vertical-align:top;position:relative;top:-4px;margin-right:4px;";
	$_add_style = "vertical-align:middle;";

	$_img = "";
	if( $_status == "new" )		  $_img = str_ireplace("replace=ifneeded;","height:{$_height}px;{$_add_style}",$_img_banner_new);
	if( $_status == "approved" ) $_img = str_ireplace("replace=ifneeded;","height:{$_height}px;{$_add_style}",$_img_banner_approved);
	if( $_status == "rejected" ) $_img = str_ireplace("replace=ifneeded;","height:{$_height}px;{$_add_style}",$_img_banner_rejected);

	if( $_id	){
		$_img = str_ireplace("<img","<img	id='{$_id}'	",$_img);
	}


	$_img = str_ireplace("/>"," title='".keys_to_translations($_status)."' />",$_img);

	if( $_status == "approved" or $_status == "rejected" ){
		#vd($_img);
	}
	

	$_str_ret	= "";
	if( $_add_title1 ) $_str_ret .= "<span style='font-size:10px;'>$_add_title</span><br>";
	$_str_ret .= $_img;	
	
	return $_str_ret;
}


function makeRandomMailKey( ){
 	srand((double)microtime()*1000000);
	$salt = "abchefghjkmnpqrstuvwxyz0123456789";
	$i = 0;
	while ($i <= 7 ){
		$num = rand() % strlen($salt);
		$tmp = substr($salt, $num, 1);
		$pass = $pass . $tmp;
		$i++;
	}
	return $pass;
}

# 33,20 => 33.20
#	Wenn die locale auf deutsch gesetzt wird,
#	Werden Berechnungen, wie bspw. $_diff = $x2 - $x1 mit Komma als Separator zurückgegeben.
#	Eine MySQL-Query mit WHERE param=0,2 gilt aber als Fehler.
#	Daher müssen die Kommata nach arithmethischen Operationen wieder entfernt werden.
#	Das ist besser, als auf die Locale-Eigenschaften zu verzichten
function kommata_zu_punkten($_arg ){
	if( isset($_arg) ){
		// Kommata zu Punkten
		return str_replace( "," , "." , $_arg);
	}else{
		#echo "not set<br>";
	}
}


# convenience function, to check for a particular request value 
#	and return it, if it is set
# param $_admit_0_as_value => integer 0 or string "0" counts as value
function request_isset_and_not_empty( $_key, $_admit_0_as_value	= false ){

	$_ret = false;


	if( isset( $_REQUEST[$_key] ) ){
		
		if( $_REQUEST[$_key]	){
			$_ret = $_REQUEST[$_key];
		}
		
		if( $_admit_0_as_value ){
			if( $_REQUEST[$_key] === 0	 ) $_ret = $_REQUEST[$_key];
			if( $_REQUEST[$_key] ==	"0" ) $_ret = $_REQUEST[$_key];
		}
		
	}

	return $_ret	;
	
} 





# convenience function, to check for a particular value 
# param $_admit_0_as_value => integer 0 or string "0" counts as value
function array_key_isset_and_not_empty( $_arr, $_key, $_admit_0_as_value	= false ){

	$_ret = false;

	if( isset( $_arr) ){
		if( isset( $_arr[$_key] ) ){
			if( $_arr[$_key]	){
				$_ret = $_arr[$_key];
			}
			if( $_admit_0_as_value ){
				if( $_arr[$_key] === 0	 ) $_ret = $_arr[$_key];
				if( $_arr[$_key] ==	"0" ) $_ret = $_arr[$_key];
			}
			
		}
	}
	return $_ret	;
	
} 



# convenience function, to check for a particular request value 
#	and return it, if it is set
# param $_admit_0_as_value => integer 0 or string "0" counts as value
function scalar_isset_and_not_empty( $_variable, $_admit_0_as_value	= false ){

	$_ret = false;

	if( isset( $_variable ) ){
		
		if( $_variable	){
			$_ret = $_variable;
		}
		
		if( $_admit_0_as_value ){
			if( $_variable === 0	 ) $_ret = $_variable;
			if( $_variable ==	"0" ) $_ret = $_variable;
		}
		
	}
	return $_ret	;
	
} 



function get_param( $_key, $_default_value = false, $_admit_0_as_value	= false ){
	$_check_if_exists = request_isset_and_not_empty($_key);
	if( $_check_if_exists !== false ) $_ret = $_check_if_exists;
	else $_ret = $_default_value;
	
	if( is_array($_ret) ){
		$_ret_cleansed = array();
		foreach($_ret as $_key => $_val)	{
			$_ret_cleansed[ cleanse_param($_key) ] = cleanse_param($_val);
		}
		$_ret = $_ret_cleansed;
		
	} else {
		$_ret = cleanse_param($_ret);
	}
	
	return $_ret;
	
}



function fn_trimhtml( &$value, $key ){
	$value = trim(htmlspecialchars($value, ENT_QUOTES));
}  


# only removing the magic escape sequences
# use escape_for_sql() instead
#	only used ONCE - actually acting as a mere stripslashes()
function unescape_from_post($_str ){
	if( get_magic_quotes_gpc() == 1) $_str = stripslashes($_str);
	return $_str;
}

# two functions to transform single field values coming from http requests 
#	and going to the database.
#	see http://de2.php.net/manual/de/function.addslashes.php

# removes "magic quotes" escaping from PHP from GET and POST variables 
#	AND prepares for inserting/updating into database

#	for reasons that I do not understand, we have to take additional care of apostrophes
#	I assume it's mainly because we delimit the SQL statements with single quotes (= apostrophes)
#	The \' goes as ' into the database column

#	the function should be applied immediately BEFORE saving to database to avoid
#	because it changes strings towards EQUAL-test may fail after it

function escape_for_sql($_arg_mixed ){

	# handle case of an empty array
	if( is_array($_arg_mixed) and sizeof($_arg_mixed )==0 ){
		return $_arg_mixed;
	}

	
	if( is_array($_arg_mixed) and sizeof($_arg_mixed ) ){
		$_arr_return = array();
		foreach($_arg_mixed as $_key => $_str ){
			$_str = escape_for_sql($_str);
			#if( get_magic_quotes_gpc() == 1) $_str = stripslashes($_str);
			#mysql_real_escape_string($_str);
			#$_str = str_replace("'","\'",$_str);
			#$_str = str_replace("\\\\'","\'",$_str);		# prevent double escapation: \\'	changed back to \'
			$_arr_return[$_key] = $_str;			
		}
		return $_arr_return;
	} else {
		$_str = $_arg_mixed;
		if( get_magic_quotes_gpc() == 1) $_str = stripslashes($_str);
		mysql_real_escape_string($_str);
		$_str = str_replace("'","\'",$_str);
		$_str = str_replace("\\\\'","\'",$_str);		# prevent double escapation: \\'	changed back to \'
		
		return $_str;		
	}
	
}













/**
 * A function for retrieving the Kölner Phonetik value of a string
 *
 * As described at http://de.wikipedia.org/wiki/Kölner_Phonetik
 * Based on Hans Joachim Postel: Die Kölner Phonetik.
 * Ein Verfahren zur Identifizierung von Personennamen auf der
 * Grundlage der Gestaltanalyse.
 * in: IBM-Nachrichten, 19. Jahrgang, 1969, S. 925-931
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the
 * GNU General Public License for more details.
 *
 * @package phonetics
 * @version 1.0
 * @link http://www.einfachmarke.de
 * @license GPL 3.0 <http://www.gnu.org/licenses/>
 * @copyright	2008 by einfachmarke.de
 * @author Nicolas Zimmer <nicolas dot zimmer at einfachmarke.de>
 */

function cologne_phon($word){
	 
	/**
	* @param	string	$word string to be analyzed
	* @return string	$value represents the Kölner Phonetik value
	* @access public
	*/
 
	//prepare for processing
	$word=strtolower($word);
	$substitution=array(
			"ä"=>"a",
			"ö"=>"o",
			"ü"=>"u",
			"ß"=>"ss",
			"ph"=>"f"
	);

	foreach( $substitution as $letter=>$substitution ){
		$word=str_replace($letter,$substitution,$word);
	}
	 
	$len=strlen($word);
	
	if( $len <= 2 ) return ;
	 
	//Rule for exeptions
	$exceptionsLeading=array(
		4=>array("ca","ch","ck","cl","co","cq","cu","cx"),
		8=>array("dc","ds","dz","tc","ts","tz")
	);
	 
	$exceptionsFollowing=array("sc","zc","cx","kx","qx");
	 
	//Table for coding
	$codingTable=array(
		0=>array("a","e","i","j","o","u","y"),
		1=>array("b","p"),
		2=>array("d","t"),
		3=>array("f","v","w"),
		4=>array("c","g","k","q"),
		48=>array("x"),
		5=>array("l"),
		6=>array("m","n"),
		7=>array("r"),
		8=>array("c","s","z"),
	);
	 
	for( $i=0;$i<$len;$i++ ){
		$value[$i]="";
		 
		//Exceptions
		if( $i==0 AND $word[$i].$word[$i+1]=="cr" ) $value[$i]=4;
		 
		foreach( $exceptionsLeading as $code=>$letters ){
			if( in_array($word[$i].$word[$i+1],$letters) ){
					$value[$i]=$code;
			}
		}
		 
		if( 		$i!=0  AND (in_array($word[$i-1].$word[$i],$exceptionsFollowing) ) ){
			$value[$i]=8;
		} 
		 
		//Normal encoding
		if( $value[$i]==""){
			foreach ($codingTable as $code=>$letters ){
				if( in_array($word[$i],$letters))$value[$i]=$code;
			}
		}

	}
	 
	//delete double values
	$len=count($value);
	 
	for ($i=1;$i<$len;$i++){
		if( $value[$i]==$value[$i-1]) $value[$i]="";
	}
	 
	//delete vocals
	for ($i=1;$i>$len;$i++){//omitting first characer code and h
		if( $value[$i]==0) $value[$i]="";
	}
	 
	 
	$value=array_filter($value);
	$value=implode("",$value);
	 
	return $value;
	 
}



function soundex_lang_multi_word($_paragraph, $_lang, $_include_wordcounts = false){

		$_lp_value = $_paragraph;

		$_lp_value = replace_non_ascii($_lp_value," ");
		$_lp_value = replace_all_whitespace_with($_lp_value," ");
		$_arr_words = explode(" ",$_lp_value);

		
		$_arr_phon = array();
		foreach($_arr_words as $_key_unused => $_word){
			
			# for each particular languages - we use an appropriate soundex function
			if(  $_lang == 'de' ){
				$_lp_phon = cologne_phon($_word);
			} else {
				$_lp_phon = soundex($_word);
			}
			
			
			if(!$_lp_phon) continue;
			#vd("$_word => $_lp_phon");
			if( isset($_arr_phon[$_lp_phon]) ){
				$_arr_phon[" $_lp_phon"]++;			# forcing array string indexing
			} else {
				$_arr_phon[" $_lp_phon"] = 1;			# forcing array string indexing
			}
		}

		ksort($_arr_phon);	# string indexing for desired sort result:  41 441 55

		#vd($_arr_phon);
		
		$_str_ret = "";
		foreach($_arr_phon as $_lp_key => $_lp_count ){
			$_display_count = "";
			if( $_include_wordcounts  AND $_lp_count > 1 ) $_display_count = " ($_lp_count)";
			$_lp_key = trim($_lp_key);
			$_str_ret .= " $_lp_key{$_display_count}";
		}
		
		return trim($_str_ret);
		
}
	
	
	
function initial_fill_soundex_lang(){
	
		$_arr_pk_key_cols = array(
			 "id"
		);

		$_q_fill_soundex = " 
			select * from tbl_multi_lang_string 
			where 
				(length(soundex_lang) < 2 )
				
				OR 1=1
			";

		$_q_fill_soundex = " 
			select * from tbl_multi_lang_string 
			where 
				lang='en'
			";
		$_arr_fill_soundex = execute_query($_q_fill_soundex,'id');

		foreach($_arr_fill_soundex as $_lp_id => $_arr_lp){
			$_lp_val  = $_arr_lp['value'];
			$_lp_lang = $_arr_lp['lang'];

			$_lp_arr['id']	  = $_lp_id;
			$_lp_arr['soundex_lang']   = soundex_lang_multi_word($_lp_val,$_lp_lang);
			
			vd("$_lp_id - $_lp_val => $_lp_arr[soundex_lang]");

			insert_update($_lp_arr, $_arr_pk_key_cols,"tbl_multi_lang_string");
			
		}
	
}












function ARRAY_FUNCTIONS(){
}


# extends an array with "prev" and "next" keys
#	which contain some value from the previous
#	and next elements
function extending_with_prev_next(&$_some_array, $_use_key = 'id'){
	
	if( ! isset( $_some_array ) ) return;
	if( ! is_array( $_some_array ) ) return;
	
	
	
	$_id_prev = 0;
	$_arr_prev = array();
	foreach($_some_array as $_key => $_arr_lp){
		$_arr_prev[$_key] = $_id_prev;
		$_id_prev = $_arr_lp[$_use_key];
	}		
	#vd($_arr_prev);


	$_el_first = reset($_some_array);
	$_key_prev = 0;
	if( is_array($_el_first) ) $_key_prev = key( $_el_first) ;
	$_arr_next = array();
	foreach($_some_array as $_key => $_arr_lp){
		$_arr_next[$_key_prev] = $_arr_lp[$_use_key];
		$_key_prev = $_key;
	}
	$_arr_next[$_key_prev] = 0;
	#vd($_arr_next);


	foreach($_some_array as $_key => $_arr_lp){
		$_some_array[$_key]['prev'] = $_arr_prev[$_key];
		$_some_array[$_key]['next'] = $_arr_next[$_key];
	}
	
}



/*
	"unflattenes" an array based on any key that has $_hierarchify_operator inside of it.
	
		Array(
				[b_data__personenzahl] => 2
				[b_data__bnr] => 3073
		)
		
	
			Array(
				[b_data] => Array
					(
						[personenzahl] => 2
						[bnr] => 3073
					)
			)

*/
function hierarchify_array($array){
	global $_hierarchify_operator;

	$_seplen = strlen($_hierarchify_operator);

	if( !is_array($array) ){
		return NULL;
	}
	$ret_array = array();
	while( (list($key, $val) =	each($array)) ){
		
		
		if( $pos = strpos( $key, $_hierarchify_operator) ){
			$sub_key = substr($key, $pos+$_seplen);
			$sub_array_name = substr($key,0, $pos);
			if( array_key_exists($sub_array_name, $ret_array) ){
				#vd(" add to existing sub");
				$ret_array[$sub_array_name] = $ret_array[$sub_array_name] + array($sub_key =>$val);
			} else {
				#vd(" add new sub");
				$ret_array = $ret_array + array($sub_array_name=>array($sub_key =>$val));
			}
		}
		else {
			$ret_array = $ret_array + array($key=>$val);
		}
		
	}
	return $ret_array;
}



# takes away any string keys - instead integers keys are 
# allocated in subsequent order
function array_reorganize_stringkeys_to_integers($_arr_arg ){
	$i = 0;
	$_arr_ret = array();
	foreach($_arr_arg as $_key => $_val ){
		$_arr_ret += array($i=>$_val);
		$i++;
	}
	return $_arr_ret;
}

/*
	hierarchical arrays have a subarray in each key
	
	this function reorganizes such an array by a key from the subarray (child array)
	
	if $_flatten is set to true, the value of this subkey will 
	replace the entire sub-array - we have a one-dimensional array after that
		with INTEGER indexing

	the new organizational key may consist of MULTIPLE subkeys
	
	there is also a "collision" control - the new organization should not 
		drop or condense any records
		
	if collisions DO occur, another key may indicate which record should be kept

	dropped records are saved into a key 'dropped_records'
	
	application would be:
		we have an array of mutliple image descriptions for multiple images for multiple apartments
		
		we may now reorganize the array by "apartmentNumber__imageNumber__descriptionId"
		
		if we had description in several versions, then we may specify that the highest version-description
		 should be keep: $_on_multiple_keys_keep_max_of = 'version'
		 
		 older versions would be put into a 'dropped_records' subkey for each array record
		
		
		[2873__5380__en] => Array(
			[main_id] => 2873
			[img_id] => 5380
			[desc_id] => 42
			[value] => velvet sofa, red carpet, red curtain - very tasteful
			[desc_last_changed] => 2011-10-24
			[dropped_records] => Array
				 (
					[0] => Array
						(
						 [value] => couch table from ecologigal wood
						 [desc_last_changed] => 2011-10-23
						)
	
					[1] => Array
						(
						 [value] => bathroom scrubbed
						 [desc_last_changed] => 2010-02-14
						)
	
				 )
	
		 )		

*/
function array_reorganize_by_subkey($_arr_arg, $_mixed_subkey, $_arr_options = array() ){
	
	
	$_flatten = false;		
	$_on_multiple_keys_keep_max_of = '';			# if muliple occurrences of a subkey occur, keep the 
	extract( expand_options_array($_arr_options) );
	
	
	$_subkey = $_mixed_subkey;

	global $_hierarchify_operator;


	$_multi_key = false;
	if( is_array($_mixed_subkey) AND sizeof($_mixed_subkey) > 0 ){
		$_multi_key = true;
	} 
	
	
	if( is_array($_arr_arg) AND sizeof($_arr_arg) ){
		$_arr_return = array();
		foreach($_arr_arg as $_key => $_arr_sub ){
			
			if( $_flatten	){

				if( $_multi_key ){
					print_error('this combination is not implemented');
				} else {
					$_arr_return[] = $_arr_sub[$_subkey];	
				}

			} else {

				if( $_multi_key ){
					$_lp_key_total = false;
					foreach($_mixed_subkey as $_key_unused => $_lp_subkey ){
						if( $_lp_key_total !== false ) $_lp_key_total .= $_hierarchify_operator;
						$_lp_key_total .= $_arr_sub[$_lp_subkey];
					}

					if( $_arr_return[ $_lp_key_total ]	){
						if( $_on_multiple_keys_keep_max_of ){
							$_prev_max = $_arr_return[ $_lp_key_total ][$_on_multiple_keys_keep_max_of];
							$_curr_val = $_arr_sub[$_on_multiple_keys_keep_max_of];
							#vd("check $_prev_max vs. $_curr_val");
							if( $_curr_val == $_prev_max ){
								print_error(": key -$_lp_key_total- has1 DUPLICATE values - therfore the max rule is not applicable - for key $_on_multiple_keys_keep_max_of ($_prev_max vs. $_curr_val).");
								#vd($_mixed_subkey);
								#vd($_arr_arg);
							}
							if( $_curr_val > $_prev_max	){
								
								$_arr_tmp = $_arr_return[ $_lp_key_total ];
								
								$_arr_return[ $_lp_key_total ] = $_arr_sub;

								$_arr_return[ $_lp_key_total ]['dropped_records'][] = $_arr_tmp;
								#vd("assigned new val: $_curr_val against $_prev_max");
							} else {
								#vd("keeping existing val $_prev_max against $_curr_val");								
								$_arr_return[ $_lp_key_total ]['dropped_records'][] = $_arr_sub;
								continue;					
							}
						} else {
							print_error(": key -$_lp_key_total- has2 DUPLICATE values 
								- and there is no rule to decide which one to choose.");
							#$_arr_return[ $_lp_key_total ]
							#vd($_mixed_subkey);
							#vd($_arr_arg);
							#vd(debug_backtrace());
						}
					}
					$_arr_return[ $_lp_key_total ] = $_arr_sub;	

				} else {
					$_tmp_sk = $_arr_sub[$_subkey];
					if( isset( $_arr_return[ $_tmp_sk ] )  AND  $_arr_return[ $_tmp_sk ] ){
						print_error(": key $_arr_sub[$_subkey] has3 DUPLICATE values. Rows are condensed and dropped !!! ");
					}
					$_arr_return[ $_arr_sub[$_subkey] ] = $_arr_sub;	

				}



			}
		}	
		return $_arr_return;	
	} else {
		return $_arr_arg;	
	}
}



# we want many function arguments to be either scalar value or array
# inside the function implementation we always extend scalar value to array
function standardize_to_array($_arg){
	if( is_array($_arg) ){
		return $_arg;
	} else if( ! isset($_arg) ){
		return array();	
	} else {
		return array($_arg);	
	}
	
}

/*
	transforms values with integer keys into keys themselves with value true
	
	=> thus config arrays can be written shorthand, 
		instead of typing $_arr_options = array( "option_x" => true )
		we can just note	$_arr_options = array( "option_x" )
		
	key-values with string-keys remain unchanged.	
	
	we also add an leading underscore
	
	param $_prefix is an ADDED prefix to the variable names
	
*/
function expand_options_array( $_arr_input = array() , $_prefix = '', $_single_scalar_key = 'other_style' ){
	
	$_arr_ret = array();
	
	# single scalar value is accepted as well; transformed into array
	if( ! is_array($_arr_input) AND strlen($_arr_input)>0 ){
		$_arr_input	= array($_single_scalar_key => $_arr_input);
	}
	
	if( is_array($_arr_input)	AND ( sizeof($_arr_input) > 0	) ){
		foreach( $_arr_input as $_key => $_val ){
			if(	is_int( $_key )	){
				if( substr($_val,1) <> "_" ) $_val = "_". $_val;
				$_arr_ret[$_prefix.$_val] = true;	# make the value a key
			} else {
				if( substr($_key,1) <> "_" ) $_key = "_". $_key;
				$_arr_ret[$_prefix.$_key] = $_val;	# unchanged
			}			
		}
	}
	

	return $_arr_ret; 		
	
}




function get_current_editable_languages($_arr_options = array() ){

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 

	$_as_js_map = false;
	extract( expand_options_array($_arr_options) );


	$_arr_lang_for_pop = $_LANGUAGES;

	$_arr_languages_editable[0] = get_param('lg0',$_CUR_LOCALE);
	
	unset($_arr_lang_for_pop[ $_arr_languages_editable[0] ]);


	for( $_i = 1; $_i < $_LANGUAGES_EDITABLE; $_i++){
		$_arr_languages_editable[$_i] = get_param('lg'.$_i,array_shift($_arr_lang_for_pop) );
		#vd($_arr_languages_editable);
	}	
	

	if( $_as_js_map ){
		$_str_ret = "";
		foreach($_arr_languages_editable as $_lg_index => $_lp_lg){
			$_str_ret .= " map_changes_by_id['lg{$_lg_index}'] = '$_lp_lg'; \n\t\t\t\t"; 
		}
		return $_str_ret;
	}else{
		return $_arr_languages_editable;	
	}
}

# split by KEY
function split_array_by($_arr, $_split_key){

	$_arr_ret[0] = array();
	$_arr_ret[1] = array();

	$_splitting_point_reached = 0;

	foreach($_arr as $_key => $_arr_lp){
		if( $_key == $_split_key ){
			$_splitting_point_reached = 1;
			continue;	
		}
		$_arr_ret[$_splitting_point_reached][$_key] = $_arr_lp;
	}

	return $_arr_ret;
	
}

# http://stackoverflow.com/questions/4665782/call-time-pass-by-reference-has-been-deprecated
# the ampersand is only legacy PHP4.0 ?
function get_first_key($_arr){
	reset($_arr);				# deprecated as Call-time pass-by-reference
	$_first_key = key( $_arr );
	return $_first_key;
}
function get_last_key($_arr){
	end($_arr);					# deprecated as Call-time pass-by-reference
	$_last_key = key( $_arr );
	return $_last_key;
}



function get_first_val($_arr){
	get_last_val($_arr);
}
function get_last_val($_arr){
	print_error("trivial - just use reset() or end()");	
}


function log_request_data($_arg_page_name, $_arg_unr = "none" , $_img_gallery='00' ){

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 
	
	if( $config['activate_request_logging'] ){
		# continue	
	} else {
		return;	
	}

	$_arr_pk_key_cols = array(
		  "ort" 
		, "page_name" 
		, "unr" 
		, "lang" 
		, "col_year_month" 
		, "personenzahl" 
		, "zimmer" 
		, "lage" 
		, "referer" 
		, "other" 	
	);



	$_arr_params_extended = array(
		 "unterkunftArt"
		,"_etage_0"
		,"_etage_1"
		,"_etage_2"
		,"_etage_3"
		,"_etage_4"
		,"_etage_5"
	);

	$_arr_params_extended += $_search_extras;

	
	
	$_ref = isset( $_SERVER['HTTP_REFERER'] )? $_SERVER['HTTP_REFERER'] : '';
	$_ref = substr( $_ref ,7, 90 );
	$_pos_question_mark = strpos($_ref,"?");
	if( $_pos_question_mark ) $_ref = substr( $_ref,0, $_pos_question_mark );
	$_ref = str_ireplace( ".php","", $_ref);
	$_ref = str_ireplace( "www.","", $_ref);
	

	$_pos_vermieter_bereich = strpos($_ref,$_href_ll_root);
	if( $_pos_vermieter_bereich  and starts_with($_ref,$TR['domain_www']) ) return;
	
	
	
	if( starts_with( $_ref, "google.de" ))	   $_ref = "google.de";
	else if( starts_with( $_ref, "google.com" )) $_ref = "google.com";
	else if( starts_with( $_ref, "google." ))	$_ref = "google other";

	if( starts_with( $_ref, "maps.google." ))		$_ref = "maps.google.xxx";
	if( starts_with( $_ref, "images.google." ))		$_ref = "images.google.xxx";

	if( strpos( $_ref, "yahoo." )	!== false)	$_ref = "yahoo";
	if( strpos( $_ref, "mail.live." )!== false )   $_ref = "mail.live";


	
	if( starts_with( $_ref, "facebook." ))  $_ref = "facebook";
	if( starts_with( $_ref, "m.facebook." ))	$_ref = "facebook";
	if( starts_with( $_ref, "mowitania." ))	 $_ref = "mowitania.de/kulturpl";
	if( starts_with( $_ref, "kulturplanung." )) $_ref = "mowitania.de/kulturpl";
	if( starts_with( $_ref, "kulturweg." )) $_ref = "kulturweg";
	if( starts_with( $_ref, "ferienwohnungen-wien." )) $_ref = "ferienwohnungen-wien";
	if( starts_with( $_ref, "ferienwohnung-prag." ))   $_ref = "ferienwohnung-prag";
	if( starts_with( $_ref, "ferienwohnung-london." ))   $_ref = "ferienwohnung-london";
	
	
	if( starts_with( $_ref, "pension.de" ))   $_ref = "pension.de";
	if( starts_with( $_ref, "deutsche-pensionen.de" ))   $_ref = "deutsche-pensionen.de";
	if( starts_with( $_ref, "djstools.com" ))   $_ref = "djstools.com";

	if( starts_with( $_ref, "192.168.1.3/" ))   return;

	if( starts_with( $_ref, "/cms/" )) $_ref = "/cms";
	
	if( starts_with( $_ref, "/unterkunft/" ))	$_ref = "/unterkunft/xxx";
	
	
	
	$_pos_forward_slash = strpos($_ref,"/");
	if( $_pos_forward_slash ) $_ref = substr( $_ref,0, $_pos_forward_slash );


	$_arr_insert_update = array(
		 "page_name" => $_arg_page_name
		,"unr"	    => $_arg_unr
		,"lang"	    => $_img_gallery	# changed - 'de' #$_CUR_LOCALE
		,"col_year_month"=> intval(date( "y")*100 + date( "m"))
		,"ort"	   => 'ostsee'
		,"personenzahl"  => get_param('beds_min')
		,"zimmer"		=> get_param('rooms_min')
		,"lage"		  => implode(",",get_param('category[]',array()) )
		,"referer"	   => $_ref
		,"count"		 => 0
	);
		
		
	$_arr_insert_update['other'] = "";
		
	foreach($_arr_params_extended as $_idx => $_lp_key){
		if( request_isset_and_not_empty( $_lp_key )  ){
			$_arr_insert_update['other'] .= "&".$_idx."=".get_param($_lp_key);
		}
	}
	
	insert_update(
		  $_arr_insert_update
		, array("force_insert")
		,"t_request_log"
		, array("insert_ignore" => "IGNORE" , "log_suppress" , "skip_warning" => 1366 )
	);

	#vd($_arr_insert_update);
	
	$_arr_insert_update['count'] = "do_not_enclose count+1";
	
	insert_update(
		  $_arr_insert_update
		, $_arr_pk_key_cols
		,"t_request_log"
		, array("log_suppress" )
	);
	
	
}

function log_request_data_search_request(){

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 
	
	if( $config['activate_request_logging'] ){
		# continue	
	} else {
		return;	
	}

	$_arr_pk_key_cols = array(
		  "ort" 
		, "page_name" 
		, "unr" 
		, "lang" 
		, "col_year_month" 
		, "personenzahl" 
		, "zimmer" 
		, "lage" 
		, "referer" 
		, "other" 	
	);



	$_arr_params_extended = array(
		 "text"
		,"type"
		,"beach_distance"
	);

	$_arr_params_extended = array_merge( $_arr_params_extended, array_keys($_search_extras) );


	$_cat = get_param('category',array());
	if( ! is_array($_cat) AND $_cat)  $_cat = array($_cat) ; # legacy scalar val
	
	

	$_arr_insert_update = array(
		 "page_name" => 'srl'
		,"unr"	   => 'none'
		,"lang"	  => 'de' #$_CUR_LOCALE
		,"col_year_month"=> intval(date( "y")*100 + date( "m"))
		,"ort"	   => 'ostsee'
		,"personenzahl"  => get_param('beds_min')
		,"zimmer"		  => get_param('rooms_min')
		,"lage"		  => implode(",",$_cat )
		,"referer"	   => "not_differentiated"
		,"count"		 => 0
	);
		
	
	$_arr_extra = 	get_param("extra");
	$_arr_insert_update['other'] = "";
	
		
	foreach($_arr_params_extended as $_idx => $_lp_key){
		if( get_param($_lp_key)  ){
			$_arr_insert_update['other'] .= "&".$_idx."=".get_param($_lp_key);
		}
		if( $_arr_extra[$_lp_key]  ){
			$_arr_insert_update['other'] .= "&".$_idx."=".$_arr_extra[$_lp_key];
		}
	}
	
	insert_update(
		  $_arr_insert_update
		, array("force_insert")
		,"t_request_log"
		, array("insert_ignore" => "IGNORE" , "log_suppress" , "skip_warning" => 1366)
	);

	
	$_arr_insert_update['count'] = "do_not_enclose count+1";
	
	insert_update(
		  $_arr_insert_update
		, $_arr_pk_key_cols
		,"t_request_log"
		, array("log_suppress" )
	);
	
	
}





function log_request_image($_arg_unr = -1 , $_which = 'apartment_detail' ){


	$_arr_insert_update = array(
		 "property_id"	    => $_arg_unr
	);
		
	
	insert_update(
		  $_arr_insert_update
		, array("force_insert")
		,"t_request_log_image"
		, array("insert_ignore" => "IGNORE" , "log_suppress" )
	);

	if( 		$_which == "apartment_detail"  ){
		$_arr_insert_update['count_visit_detail'] = "do_not_enclose  count_visit_detail+1";			
	}
	else if( $_which == "image_gallery"  ){
		$_arr_insert_update['count_visit_image_gallery'] = "do_not_enclose count_visit_image_gallery+1";			
	}
	else if( $_which == "image_single"  ){
		$_arr_insert_update['count_visit_image_single'] = "do_not_enclose count_visit_image_single+1";			
	} else {
		print_error("apartment_detail, or visit_image_gallery or visit_image_single");
	}
	
	
	insert_update(
		  $_arr_insert_update
		, "property_id"
		,"t_request_log_image"
		, array("log_suppress" )
	);
	
	
}




/*
	http://stackoverflow.com/questions/2282477/mapping-latitude-and-longitude-values-onto-an-image

	we need the left/top and bottom/right lat/long coordinates of a map

	then we can plot
	
	transforms latitude - longitude to linear coordinates
	
	
*/
function get_x_y_from_lat_long($pointLon,$pointLat){
	$imageWidth = 1024;
	$imageHeight = 283;
	
	$_after_touch_cx = 204;
	$_after_touch_cy =  13;

	/* with google maps we estimate, that our image has the following coordinates:
		left-top:  		54.8,9.175
		bottm-right:	53.78,16.05
	*/

	$_cx = 0.045;
	$ImageExtentLeft   =  9.11	+ $_cx;
	$ImageExtentRight  = 15.302	+ $_cx;




	$_cy = 0.015;
	$ImageExtentTop	= 54.799 + $_cy;
	$ImageExtentBottom = 53.805 + $_cy;

	$ImageExtentTop	= 54.795 + $_cy;
	$ImageExtentBottom = 53.800 + $_cy;



	$x = $imageWidth  *  (		$pointLon - $ImageExtentLeft  ) / ($ImageExtentRight - $ImageExtentLeft   )  ;
	$y = $imageHeight *  ( 1 - (  $pointLat - $ImageExtentBottom) / ($ImageExtentTop   - $ImageExtentBottom) );

	$x = floor($x);
	$y = floor($y);

	$x += $_after_touch_cx;
	$y += $_after_touch_cy;

	return array(x=>$x, y=>$y);
}




function xml_format($dom_or_element){

	if( ! is_object($dom_or_element) ){ 
		print_error("argument is a string - not an object - or it is empty");
		return; 
	};

	# Code to format XML after appending data
	$xml_unformatted = $dom_or_element->ownerDocument->saveXML($dom_or_element); # put string in outXML 
	
	#	return $xml_unformatted;
	
	if( strlen($xml_unformatted) > 1  AND substr($dom_formatted,1,1) === "<" ) {
		
		# now create a brand new XML document
		$dom_formatted = new DOMDocument(); 
		$dom_formatted->preserveWhiteSpace = false; 
		$dom_formatted->formatOutput = true; 	# try to make this format again
		$dom_formatted->loadXML($xml_unformatted);  # pass the output of the first bunch of stuff we did to the new XML document:	
		# now cleanly formatted output without using LIBXML_NOBLANKS - which may have undesired consequences
	
		$_number_of_lines	= $dom_formatted->save('output1.xml'); # save as file 
		$_xml_formatted = $dom_formatted->saveXML(); # save as file 

		return $_xml_formatted;

	} else {
		return $xml_unformatted;
	
	}
	


}

function xml_append_node($dom){

	$root = $dom->firstChild;
	$list = $root->childNodes->item(1);


	$row = $dom->createElement('loc');
	$list->appendChild($row);
	$song = $dom->createElement('song'); 
	$row->appendChild($song);
	$song->setAttribute('artist',  $_REQUEST['name'] . "_name" );
	
	$track      = $_REQUEST['track']. "track";
	$wcm_node = $dom->createTextNode($track);
	
	$song->appendChild($wcm_node);
	
}







# shortcut to print_error
#		WITH stack trace, WITH output to http response
#		Variable Dump with Stack trace
function vds($_str, $_arr_options = array() ){
	$_arr_options['_print_error_always'] = 1;
	$_arr_options['_with_stack_trace'] = 1;
	vd($_arr_options);
	return print_error( $_str ,  $_arr_options);
}


# print database error
function printError( $str, $link  ){ 
  printf( "<font color='red'>Err: %s -- %s: %s</font><BR>\n", 
	$str, mysql_errno($link),  mysql_error($link) ); 
}	

function print_error($_str, $_arr_options = array() ){
	
	if( ! $_str ) return "";

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 


	# defaults for options
	$_with_stack_trace = false;
	$_filter_newlines  = false;
	$_do_return			 = false;
	$_suppress_function_name = false;
	$_enduser = false;
	extract( expand_options_array($_arr_options) );


	$_arr_backtrace = debug_backtrace();
	$_trace_data_this_func = array_shift( $_arr_backtrace );
	if( 		$_arr_backtrace[0]['function'] == 'log_message' 
			OR	$_arr_backtrace[0]['function'] == 'require_once' 
	){
		# dont output log_message; shift again
		$_trace_data_log_message = array_shift( $_arr_backtrace );		
	}
	$_name_of_fct = $_arr_backtrace[0]['function'];
	if($_name_of_fct) $_name_of_fct = "{$_name_of_fct}(): ";
	if($_suppress_function_name OR $_enduser) $_name_of_fct = "";
	
	#$_str = str_replace(". ",". <br>",$_str);	# 3. Jan would cause new line
	$_str = str_replace("Nr. <br>","Nr. ",$_str);
	$_str = str_replace("! ",". <br>",$_str);
	
	$_str_dis = "
			<div 
				id='wrapper_error'
				style='
					display:block;
					position:relative;
					width:99%;
					margin: 8px auto;
					padding:8px;
					background-color:{$_COLOR_FLASH_AND_ERROR};
					color:#000;
					font-weight:bold;
				'>{$_name_of_fct}{$_str}
				</div>";

	

	if( 		(  isset($_GET['dbg'])  AND $_GET['dbg'] > 10 )
			OR	$_with_stack_trace
	){
		if( sizeof($_arr_backtrace) ) vd($_arr_backtrace);		
	}
	

	if( $_filter_newlines ){
		$_str_dis = str_ireplace("<br>","",$_str_dis);
	}

	if( $_do_return ){
		return $_str_dis;
	} else {
		echo $_str_dis;
	}

	
}


#echo "tool2 <br>";



# wrapper around error_log
function log_message($_msg, $_category=""){

	if( ! $_category ){
		$_arr_backtrace = debug_backtrace();
		if( sizeof($_arr_backtrace)>1 ){
			$_trace_data_this_func = array_shift( $_arr_backtrace );	# remove trace of THIS function
		}  
		
		$_function = $_arr_backtrace[0]['function'] ;
		$_line	  = $_arr_backtrace[0]['line'] ;
		$_file	  = $_arr_backtrace[0]['file'] ;
		
		# strip document root from beginning of file
		if( $_dr = $_SERVER['DOCUMENT_ROOT'] ) {
			$_file = substr($_file, strlen($_dr) );
		}
		
		$_category =  "$_file $_function Zeile $_line";	
	}

	global $_username;
	$_user_label = '';
	if( $_username ) $_user_label = "logged in: $_username; ";
	$_combined_line = date("Ymd H:i:s") ." $_user_label $_category <br>\n\t\t$_msg \n";
	
	# two debug levels for rendering log entries to console
	#  we use print error, as print_error also shows the stacktrace if wanted
	if( isset($_GET['dbg']) AND $_GET['dbg'] ){
			if( $_GET['dbg'] <> "msg_only")	print_error($_combined_line);		# output ALL
			else print_error($_msg);			# output only message
	} 

	if( $_category == "authentication" ){
		if( defined("AUTHENTICATION_LOG") ){		# defined() needs quotesy
			error_log( date("Ymd H:i:s") ."\t\t$_msg \n" , 3, AUTHENTICATION_LOG);	
		}else{
			print_error("Konnte keinen Auth-Log Eintrag schreiben, da kein Logverzeichnis definiert wurde.");	
		}
	}else if( $_category == "sql_sel" ){
		if( defined("LOG_DIR1") ){		# defined() needs quotesy
			error_log( date("Ymd H:i:s") ."\t$_msg\n" , 3, LOG_DIR1 . "sql_sel.log");	
		}else{
			print_error("Konnte kein sql-sel Log Eintrag schreiben.");	
		}
	}else{
		# the actual writing to log file
		if( defined("APP_LOG") ){						# defined() needs quotesy
			error_log($_combined_line , 3, APP_LOG);	
		}else{
			print_error("Konnte keinen App-Log Eintrag schreiben, da kein Logverzeichnis definiert wurde.");	
		}
	}

	
	
}




	



	function render_layout_header( 
			  $_html_title=''
			, $_arr_options = array() 
			, $_arg_arr_header_links = array()
	){


		require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 


		@header("Cache-Control: no-cache, must-revalidate"); // HTTP/1.1
		@header("Expires: Sat, 26 Jul 1997 05:00:00 GMT"); // Datum in der Vergangenheit
		@header("Last-Modified: " . gmdate("D, d M Y H:i:s") . " GMT");

		if( ! $_html_title ) $_html_title = get_script_name();

		$_str_ret = "";
		
		$_arr_additional_js   = array();
		$_arr_additional_js[] ='jquery-ui/js/jquery.js';
		$_arr_additional_js[] ='jquery-ui/js/jquery-ui.custom.min.js';


		$_arr_additional_css   = array();
		$_arr_additional_css[] = 'http://cdn.idealo.com/ipc/1/-33Wv5YsF-/css/homepage.css';
		$_arr_additional_css[] = 'jquery-ui/css/theme-idealo/jquery-ui.custom.min.css';
		extract( expand_options_array($_arr_options) );
		

		$_str_additional_css = "\n";
		foreach($_arr_additional_css as $_key => $_val){
			$_str_additional_css .= "\t<link   href='$_val' rel='stylesheet' type='text/css' />\n";
		}
		
		$_str_additional_js = "\n";
		foreach($_arr_additional_js as $_key => $_val){
			$_str_additional_js .= "\t<script  src='$_val' ></script>\n";
		}
		

		# prepare default header links:
		$_arr_header_links_default = array(
			  "href_ll_images_manage"
		);


		$_str_doctype = "<!DOCTYPE html>\n";
		$_charset = '  <meta http-equiv="Content-Type" content="text/html;charset=utf-8"> ';

		$_str_ret .= "$_str_doctype
		$_charset
	
		<title>$_html_title</title>

	<meta http-equiv='Pragma' content='no-cache'>
	<meta http-equiv='Cache-Control' content='no-cache, must-revalidate'>
	<meta http-equiv='Expires' content='0'>

		$_str_additional_js
		$_str_additional_css


		<style>
			h1,h2,h3 {
				color: #024; 			/* idealo blue - petrol */
				margin-top:   8px;
				margin-bottom:2px;
				line-height: 120%;
			}

			input[type=submit] {
				color:#ff6600;			/* idealo orange */
				font-weight:bold;
				aaborder: 2px solid #ff6600;
				font-size:16px;
				padding: 4px 8px;
				height: auto;
			}

			/*
				#002A4C lighter petrol
			*/
		</style>


	</head>
	<body style='margin:0; padding:0;'  $_body_class >
			
			
	";

	$_str_ret .= render_company_header();
	$_str_ret .= "<div id='div_content' style='padding: 8px;'>";

	return $_str_ret;
		
}	# /function header_layout201007
	



function render_layout_footer( $_arr_options = array()  ){

	$_str_ret  = "";



	$_str_ret .= "

</div> <!--/div_content -->



</body>
</html>";


	return $_str_ret;


}




function render_company_header(){

	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require 

	$_str_ret  = "";

	$_dis_dropdown = render_select("hostname", $_arr_hostnames, array(accesskey=> 'h')) ;

	$_dis_block_1 = "		<span  style='display:inline-block; min-width: 20px; margin-left: 220px; color:#fff; font-size: 18px; margin-top: 15px;'
		>	Datenbank Messungen
	</span>";

	$_hid_inp = getHiddenInputs();
	$_dis_block_2 = "		<span  style='display:inline-block; width: 260px; margin-left:  20px; color:#fff; font-size: 15Hpx; margin-top: 11px;'
		>	<form  id='frm_select_hostname_01'>
				$_hid_inp
			DB-Maschine: $_dis_dropdown
		</form>
	</span>";


	$_str_ret .= "<span style='display:inline-block; width:100%; height: 48px; background-image:url(idealo_header.png); background-color:#00377b; background-repeat:repeat-y; vertical-align:bottom; ' >
			$_dis_block_1
			$_dis_block_2
		</span>\n";


	$_str_ret .= render_primary_navigation();

	return $_str_ret;

}


function render_primary_navigation(){

	$_str_ret  = " &nbsp; ";


	$_dir = dirname(__FILE__);

	$_self = basename( get_script_name() );

	$_deepest_dir = basename( dirname( get_script_name() ) );


	$dh = opendir($_dir);
	while(  ($_lp_file = readdir($dh)) !== false  ){
		$_arr_files_1[$_lp_file] = $_lp_file;
	}
	closedir($dh);

	ksort($_arr_files_1);

	foreach(  $_arr_files_1 as $_unused => $_lp_file  ){
		if( substr($_lp_file,0,1) == "_" ){
			if( substr($_lp_file,0,2) == "__" ){
				$_arr_files_3[] = $_lp_file;
			} else {
				$_arr_files_2[] = $_lp_file;
			}
		}
	}

	$_arr_files_4 = array_merge($_arr_files_2,$_arr_files_3);

	if( $_deepest_dir == "sql_analytics_developers" ){
		$_arr_files_4 = array("_general_log.php");
	}

	foreach(  $_arr_files_4 as $_unused => $_lp_file  ){

		if( substr($_lp_file,0,1) == "_" ){

			if( substr($_lp_file,0,2) == "__" ){
				#continue;
			}

			$_desc = get_description($_lp_file);

			$_attrx = " alt=\"{$_desc}\" title=\"{$_desc}\" ";
			$_style_text1 = " font-size:13px; text-decoration:none;  ";
			$_style_text2 = " font-size:11px; text-decoration: underline; color:#ff6600; ";


			$_arr_opt1["additional_attributes"] =  $_attrx ;
			$_arr_opt1["additional_style"] = " margin-right:20px; aaborder: 2px solid #aaa " ;

			$_arr_opt2["additional_attributes"] = $_attrx ;
			$_arr_opt2["additional_style"] = " margin-right:20px; aaborder: 2px solid #aaa " ;
			
				
			$_lp_title = substr($_lp_file,1);
			if( substr($_lp_title,-4) == ".php" ) $_lp_title = substr($_lp_title,0,-4);
			$_lp_title = str_replace("_"," ",$_lp_title);
			$_lp_title = ucwords($_lp_title);


			if( $_self == $_lp_file ){
				$_str_ret .= render_inline_block( "<b style='$_style_text1'                   >$_lp_title</b>",false, $_arr_opt1 ) ;
			} else {
				$_qs = getLinkAnhang();
				$_href = $_lp_file . "?" . $_qs;
				$_str_ret .= render_inline_block( "<A style='$_style_text2'  HREF='{$_href}'  >$_lp_title</A>",false, $_arr_opt2 ) ;
			}

		}

	}

	$_arr_opt2["additional_attributes"] = "";
	$_str_ret .= render_inline_block( "<A style='$_style_text2'  HREF='/crawler/'        >PHP MyAdmin nach Maschine</A>",false, $_arr_opt2 ) ;
	$_str_ret .= render_inline_block( "<A style='$_style_text2'  HREF='/crawler-admin/'  >PHP MyAdmin nach Funktion</A>",false, $_arr_opt2 ) ;


	$_horst = getHostName();
	$_str_ret .=  "&nbsp; &nbsp; running on $_horst"; 

	$_str_ret .= "<div  style=' display: block; width: 100%; height: 6px; background-color: #ff6600; margin-top:2px;'  > </div>";

	return $_str_ret;


}



# $_width < 0.2 prompts the CSS block to be returned again
function render_inline_block( $_text, $_width=false, $_arr_options = array() ){
	
	$_str_ret  = "";
	
	$_additional_style=false;
	$_id = "";
	$_additional_attributes = "";
	$_additional_classes = "";
	
	extract( expand_options_array($_arr_options,'',"additional_style") );

	if( $_additional_classes ) $_additional_classes = " ".$_additional_classes;

	$_attr_id = "";
	if( $_id ) $_attr_id = " id='$_id' ";
	
	$_idiotic_width = false;
	if( $_width < 0.2 AND $_width > 0.01 ) $_idiotic_width = true;
	
	global $_cnt_inline_blocks;
	if( ! isset($_cnt_inline_blocks) OR $_idiotic_width ){
		$_cnt_inline_blocks = 0;
		$_str_ret .= "<style>
				.cls_inline_block
				{
					display: -moz-inline-stack;  /*	Firefox 2 doesn’t support inline-block, but it does support a Mozilla specific display property ‘-moz-inline-stack’ */
					display:inline-block;
					position:relative;
					vertical-align:middle;
					min-height:10px;
					padding: 0px;
					margin: 0px;
				}
			</style>";
	} 
	if( $_idiotic_width ) return $_str_ret;
	
	
	$_style_arg ="";
	
	if( $_width ){
		$_has_unit = false;
		if( strtolower(substr($_width,-2)) == 'px' ) $_has_unit = true;
		if( strtolower(substr($_width,-1)) == '%' )  $_has_unit = true;
		if( ! $_has_unit ) $_width .= "px";
		$_style_arg .= " width:$_width;  ";
	}
	
	if( $_additional_style ){ 
		$_style_arg .= " $_additional_style; ";
	}
	
	$_attr_val_style = "";
	if( $_style_arg )  $_attr_val_style = " style='$_style_arg' ";

	$_str_ret .= "	
		<span
			$_attr_id 
			class='cls_inline_block{$_additional_classes}'
			$_attr_val_style
			$_additional_attributes
		>$_text</span>\n";


	return $_str_ret;
}


function render_br(){
	$_str_ret = "";
	$_str_ret = render_inline_block(" ","100%",'min-height:1px; height:1px; padding: 0; margin: 0; line-height:1px;');
	return $_str_ret;
}

# read second line of a file as description
# IF it starts with #
function get_description($_lp_file){

	$_f = fopen($_lp_file,"r");
	$_line1 = fgets($_f);

	$_desc  = "";
	$_desc  = fgets($_f);
	fclose($_f);
	if( substr($_desc,0,1) == '#' ){
		$_desc = substr($_desc,1);
		$_desc = trim($_desc);
	} else {
		$_desc = 'no description available';
	}

	return $_desc;

}



function render_select($_fld_name, $_arr_key_value, $_arr_options = array() ){

	$_selected_id = get_param($_fld_name);
	if( substr($_fld_name,-2 ) == "[]")  $_selected_id = get_param( substr($_fld_name,0,-2 ) );

	$_accesskey  = "";
	$_attributes = "";
	$_style      = "";
	$_id         = "$_fld_name";

	$_event_handler = "  onchange='this.form.submit();'  ";

	extract( expand_options_array($_arr_options) );
	
	$_str_ret  = "";
	$_str_ret .= "<select  name='$_fld_name'  id='$_id'  $_event_handler  accesskey='$_accesskey'  style='padding: 1px 4px; {$_style}'  $_attributes >";


	foreach($_arr_key_value as $_key => $_val){
		$_sel = "";
		if( $_selected_id == $_key ) $_sel = "selected";
		if( is_array($_selected_id ) ){
			if( in_array($_key,$_selected_id) ){
				$_sel = "selected";
			}
		}
		$_str_ret .= "<option value='$_key' $_sel>$_val</option>";
		
	}
	$_str_ret .= "</select>";


	return $_str_ret;

}



function render_checkbox($_fld_name, $_arr_options = array() ){

	$_value = get_param($_fld_name);
	$_checked = "";
	if( $_value ) $_checked = " checked='true' ";
	if( substr($_fld_name,-2 ) == "[]")  $_value = get_param( substr($_fld_name,0,-2 ) );

	$_accesskey  = "";
	$_attributes = "";
	$_style      = "";
	$_id         = "$_fld_name";

	$_event_handler = "  onchange='this.form.submit();'  ";

	extract( expand_options_array($_arr_options) );
	
	$_str_ret  = "";
	$_str_ret .= "\t\t<input  type='hidden'    name='$_fld_name'  id='{$_id}b'  value='0'  />\n";
	$_str_ret .= "\t\t<input  type='checkbox'  name='$_fld_name'  id='{$_id}'   value='1'  $_checked  accesskey='$_accesskey'  style='padding: 1px 4px; {$_style}'  $_attributes  $_event_handler />\n";


	return $_str_ret;

}




function render_hidden($_fld_name, $_arr_options = array() ){

	$_value = get_param($_fld_name);
	$_accesskey  = "";
	$_attributes = "";
	$_style      = "";
	$_id         = "$_fld_name";

	extract( expand_options_array($_arr_options) );
	
	$_str_ret  = "";
	$_str_ret .= "\t\t<input  type='hidden'    name='$_fld_name'    value='$_value'  />\n";


	return $_str_ret;

}



	function exec_curl_multi($_url, $_cnt = 3){
	
		for( $i = 0; $i < $_cnt; $i++){
			$_arr_ch[$i] = curl_init();

			curl_setopt( $_arr_ch[$i] , CURLOPT_HEADER, 0);
			curl_setopt( $_arr_ch[$i] , CURLOPT_URL   , "{$_url}"  );
			curl_setopt( $_arr_ch[$i] , CURLOPT_TIMEOUT, 50);
			curl_setopt( $_arr_ch[$i] , CURLOPT_RETURNTRANSFER, true);

		}

		//create the multiple cURL handle
		$mh = curl_multi_init();

		for( $i = 0; $i < $_cnt; $i++){
			curl_multi_add_handle( $mh , $_arr_ch[$i] );
		}

		#vd($_arr_ch);

		$active = null;
		do {
			$mrc = curl_multi_exec( $mh, $active);
			#echo "mc1: $mrc<br>\n";
		} while( $mrc == CURLM_CALL_MULTI_PERFORM );

		while( $active && $mrc == CURLM_OK ){
			if( curl_multi_select($mh) != -1 ){
				do {
					$mrc = curl_multi_exec($mh, $active);
					#echo "mc2: $mrc<br>\n";
				} while( $mrc == CURLM_CALL_MULTI_PERFORM );
			}
		}



		for( $i = 0; $i < $_cnt; $i++){
			curl_multi_remove_handle( $mh , $_arr_ch[$i] );
		}
		curl_multi_close($mh);

	}

?>