package schema

var SchemaPro = `{
	"type":"record",
    "name":"TestRecord",
	"fields":[
		{
			"name":"c_netnum",
			"type":"int"
		},
		{
			"name":"c_ip",
			"type":"long"
		},
		{
			"name":"c_flowid",
			"type":"string",
			"default":"rrrrrrrrrrrrrortrtrtrtweoiwoewserwd"
		},
		{
			"name":"c_src_ipv4",
			"type":"long"
		},
		{
			"name":"c_src_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_src_port",
			"type":"int",
			"default":8085
		},
		{
			"name":"c_s_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_s_tunnel_port",
			"type":"int",
			"default":8080
		},
		{
			"name":"c_dest_ipv4",
			"type":"long"
		},
		{
			"name":"c_dest_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_dest_port",
			"type":"int",
			"default":8888
		},
		{
			"name":"c_d_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_d_tunnel_port",
			"type":"int",
			"default":9094
		},
		{
			"name":"c_packet_group",
			"type":"int",
			"default":11
		},
		{
			"name":"c_proto_type",
			"type":"int",
			"default":12
		},
		{
			"name":"c_connect_status",
			"type":"int",
			"default":3
		},
		{
			"name":"c_direct",
			"type":"int",
			"default":13
		},
		{
			"name":"c_server_dir",
			"type":"int",
			"default":16
		},
		{
			"name":"c_up_packets",
			"type":"long",
			"default":1452656
		},
		{
			"name":"c_up_bytes",
			"type":"long",
			"default":1546265
		},
		{
			"name":"c_down_packets",
			"type":"long",
			"default":4515154
		},
		{
			"name":"c_down_bytes",
			"type":"long",
			"default":4545751
		},
		{
			"name":"c_c2s_packet_jitter",
			"type":"int",
			"default":20
		},
		{
			"name":"c_s2c_packet_jitter",
			"type":"int",
			"default":21
		},
		{
			"name":"c_log_time",
			"type":"long"
		},
		{
			"name":"c_app_type",
			"type":"string",
			"default":"iuiuiuiuiuiurhrhrhrhrewr425refre"
		},
		{
			"name":"c_stream_time",
			"type":"long",
			"default":1663730958678
		},
		{
			"name":"c_hostr",
			"type":"string",
			"default":"cecloud"
		},
		{
			"name":"c_s_boundary",
			"type":"long",
			"default":5465115487
		},
		{
			"name":"c_s_region",
			"type":"long",
			"default":5454545498
		},
		{
			"name":"c_s_city",
			"type":"long",
			"default":56534
		},
		{
			"name":"c_s_district",
			"type":"long",
			"default":4642334
		},
		{
			"name":"c_s_operators",
			"type":"long",
			"default":6565934
		},
		{
			"name":"c_s_owner",
			"type":"string",
			"default":"ssssasdsafsafwerewrewrewrw"
		},
		{
			"name":"c_d_boundary",
			"type":"long",
			"default":1546534
		},
		{
			"name":"c_d_region",
			"type":"long",
			"default":45434
		},
		{
			"name":"c_d_city",
			"type":"long",
			"default":484534
		},
		{
			"name":"c_d_district",
			"type":"long",
			"default":4878
		},
		{
			"name":"c_d_operators",
			"type":"long",
			"default":4645
		},
		{
			"name":"c_d_owner",
			"type":"string",
			"default":"dsssasfdrqrewreerewrewrewrewwe"
		},
		{
			"name":"c_s_mark1",
			"type":"long",
			"default":4665
		},
		{
			"name":"c_s_mark2",
			"type":"long",
			"default":9165
		},
		{
			"name":"c_s_mark3",
			"type":"long",
			"default":1255
		},
		{
			"name":"c_s_mark4",
			"type":"long",
			"default":30439
		},
		{
			"name":"c_s_mark5",
			"type":"long",
			"default":40495
		},
		{
			"name":"c_d_mark1",
			"type":"long",
			"default":20934
		},
		{
			"name":"c_d_mark2",
			"type":"long",
			"default":46342
		},
		{
			"name":"c_d_mark3",
			"type":"long",
			"default":4632
		},
		{
			"name":"c_d_mark4",
			"type":"long",
			"default":4642
		},
		{
			"name":"c_d_mark5",
			"type":"long",
			"default":3622
		}
	]
}`
