# mongodberl
通过poolboy使用mongodb erlang driver(mongo client)来支持mongo replica set特性

提供两个接口供外部程序使用  
1、start_link  
   调用poolboy建立线程池  
2、get_value_from_mongo  
  通过mongoc(gen_server)调用相关接口  

    
