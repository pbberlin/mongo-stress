<?php



	require_once("bootstrap_functions.php");
	require_once("functions_helpers.php");
	require_once("functions_url_tools.php");


	require('reusable_globals.php');		# should be repeatable in function scope - therefore not ONCE but only require


	echo render_layout_header();


	if( ! $_HOST  ) {
		echo "please select host name<br>";
		echo render_layout_footer();
		exit;
	}


	if( $_HOST <> "b16.lvl.bln") {
		#echo "only on b16<br>";
		#echo render_layout_footer();
		#exit;
	}


	
	
	render_elapsed_time_since("start");

	echo "<h1>compare schema</h1>";


	$_host_1       = "b16";
	$_schema_1     =  get_param( "schema1");
	
	if( ! $_schema_1 ) {

		$_arr_t = execute_query(" show databases");
		foreach($_arr_t as $_key_unused => $_arr_lp){
			//vd($_arr_lp);
			vd($_arr_lp["Database"]);
		}
		
		echo render_layout_footer();
		exit;		
	}


	$_host_2       = "offer-db";
	$_schema_2     = "monitoring-db-monitoring";



	$DB_CONNECTION = false;
	$PUB_DB_NAME = $_schema_1;
	connectPublicDB();


	flush_http();

	$_arr_t = execute_query("
		select table_name from information_schema.tables where table_schema = '$_schema_1';
	");

	foreach($_arr_t as $_key_unused => $_arr_lp){
		$_lp_tn = $_arr_lp['table_name'];

		#$_sql = "RENAME TABLE `{$_schema_1}`.`{$_lp_tn}` TO `{$_db_dest}`.`{$_lp_tn}` ";
		
		$_sql = "
			SHOW CREATE TABLE  `{$_schema_1}`.`{$_lp_tn}`		
		";
		
		vd($_sql );
		$_arr_r = execute_query($_sql);
		vd($_arr_r);
	
	}
	flush_http();


	echo render_layout_footer();





?>