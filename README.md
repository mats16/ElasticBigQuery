[ElasticBigQuery]()
==============================


What's a ElasticBigQuery?
---------------------
ElasticBigQuery is Application for BigQuery on the Google App Engine platform.

ElasticBigQuery is designed to provide the ease of use like TreasureData to BigQuery.


Functions and features
-----------------------
- [td-js-sdk](https://github.com/treasure-data/td-js-sdk) id supported (jsonp only)
 - writeKey is disabled yet
- issuance of third-party cookie(uuid)
 - Custom domain is supported


Example
---------

### Tracking using td-js-sdk

Reference is [here](http://docs.treasuredata.com/articles/javascript-sdk)

#### 1, Install the Library
```html
<script type="text/javascript">
!function(t,e){if(void 0===e[t]){e[t]=function(){e[t].clients.push(this),this._init=[Array.prototype.slice.call(arguments)]},e[t].clients=[];for(var r=function(t){return function(){return this["_"+t]=this["_"+t]||[],this["_"+t].push(Array.prototype.slice.call(arguments)),this}},n=["addRecord","set","trackEvent","trackPageview","ready"],s=0;s<n.length;s++){var i=n[s];e[t].prototype[i]=r(i)}var a=document.createElement("script");a.type="text/javascript",a.async=!0,a.src=("https:"===document.location.protocol?"https:":"http:")+"//cdn.treasuredata.com/sdk/td-1.5.1.js";var c=document.getElementsByTagName("script")[0];c.parentNode.insertBefore(a,c)}}("Treasure",this);
</script>
```
#### 2, Initialize & Send Events to the Cloud

```html
<script type="text/javascript">
  ! function(td_database, td_table) {
    var td = new Treasure(
      {
        host: 'xxx-xxx.appspot.com',
        pathname: '/dmp/v1/event/',
        writeKey: 'dummy',
        database: td_database
      }
    );

    td.trackPageview(td_table);
  }('example', 'pageviews');
</script>
```


Open Source
-----------
If you want to add, fix or improve something, create an [issue](https://github.com/mats116/elasticbigquery/issues) or send a [Pull Request](https://github.com/mats116/elasticbigquery/pulls).

Before committing fixes we recommend running the unitests (in the boilerplate package).  This will help guard against changes that accidently break other code.  See the testing section below for instructions.

Feel free to commit improvements or new features. Feedback, comments and ideas are welcome.
