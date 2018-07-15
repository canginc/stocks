/*var BASF = localStorage.getItem("BASF");
if(BASF==null) BASF = [];
else BASF = domStringToList(BASF);

var BASF_delta = localStorage.getItem("BASF_delta");
if  (BASF_delta==null) BASF_delta = [0];
else BASF_delta = domStringToList(BASF_delta);

var HENKEL  = localStorage.getItem("HENKEL");
if ( HENKEL ==null) HENKEL = [];
else HENKEL = domStringToList(HENKEL);

var HENKEL_delta = localStorage.getItem("HENKEL_delta");
if  (HENKEL_delta==null) HENKEL_delta = [0];
else HENKEL_delta = domStringToList(HENKEL_delta);

var LINDE = localStorage.getItem("LINDE");
if(LINDE==null) LINDE = [];
else LINDE = domStringToList(LINDE);

var LINDE_delta = localStorage.getItem("LINDE_delta");
if  (LINDE_delta==null) LINDE_delta = [0];
else LINDE_delta = domStringToList(LINDE_delta);

var MERCK = localStorage.getItem("MERCK");
if( MERCK==null)  MERCK = [];
else  MERCK = domStringToList( MERCK);

var MERCK_delta = localStorage.getItem("MERCK_delta");
if  (MERCK_delta==null) MERCK_delta = [0];
else MERCK_delta = domStringToList(MERCK_delta);

var SAP = localStorage.getItem("SAP");
if(SAP==null) SAP = [];
else SAP = domStringToList(SAP);

var SAP_delta = localStorage.getItem("SAP_delta");
if  (SAP_delta==null) SAP_delta = [0];
else SAP_delta = domStringToList(SAP_delta);


var x_axis = [];
var x_axis = localStorage.getItem("x_axis");
if(x_axis==null) x_axis = [];
else x_axis = x_axis.split(",");
if(document.getElementById('priceDiff_BASF')!=null){
    var priceDiff_BASF = parseInt(document.getElementById('priceDiff_BASF').innerHTML);
    var priceDiff_HENKEL = parseInt(document.getElementById('priceDiff_HENKEL').innerHTML);
    var priceDiff_LINDE = parseInt(document.getElementById('priceDiff_LINDE').innerHTML);
    var priceDiff_MERCK = parseInt(document.getElementById('priceDiff_MERCK').innerHTML);
    var priceDiff_SAP = parseInt(document.getElementById('priceDiff_SAP').innerHTML);

    var sum =priceDiff_BASF+  priceDiff_HENKEL+  priceDiff_LINDE + priceDiff_MERCK + priceDiff_SAP ;
}

var current = new Date();
var h = fixTimeFormat(current.getHours());
var m = fixTimeFormat(current.getMinutes());
var s = fixTimeFormat(current.getSeconds());
var timestamp = h + ":" + m + ":" +s; 

var myPieChart = Highcharts.chart('barchart', {
    chart: {plotBackgroundColor: null,plotBorderWidth: null,plotShadow: false,type: 'pie'},
    title: {text: 'Sample Portfolio Composite'},
    tooltip: {pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'},
    plotOptions: {
    pie: {allowPointSelect: true,cursor: 'pointer',
    dataLabels: {enabled: true,format: "<b>{point.name}</b>: {point.percentage:.1f} %",
    style: {color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'}}}},
    series: [{
    name: 'Price',
    colorByPoint: true,
    data: [{name: 'BASF',y: priceDiff_BASF /sum, x: priceDiff_BASF}, 
           {name: 'HENKEL',y: priceDiff_HENKEL/sum,sliced: true,selected: true}, 
           {name: 'LINDE',y: priceDiff_LINDE /sum}, 
           {name: 'MERCK',y: priceDiff_MERCK /sum}, 
           {name: 'SAP',y: priceDiff_SAP /sum}    ]}]
});
    do not use: series: [{name: 'BASF',data: priceDiff_BASF},
             {name: 'SAP',data: priceDiff_SAP}	]
var myLineChart = Highcharts.chart('linechart', {
    chart: {type: 'spline'},
    title: {text: 'Trend of Price differences'},
    xAxis: {categories: x_axis},
    yAxis: {title: {text: 'Trend'}},
    series: [{name: 'BASF',data: BASF_delta},
             {name: 'HENKEL',data: HENKEL_delta},
             {name: 'LINDE',data: LINDE_delta},
             {name: 'MERCK',data: MERCK_delta},
             {name: 'SAP',data: SAP_delta}	]
});

function fixTimeFormat(i){
    return ( i < 10 ) ? "0" + i : i;
}
function domStringToList(dom){
    var temp = dom.split(",");
    var result = [];
    for(i=0;i<temp.length;i++)
        result.push(parseInt(temp[i]));
    return result;

}
function newrecord() { 
    BASF.push( priceDiff_BASF);
    localStorage.setItem("BASF", BASF);
    HENKEL.push( priceDiff_HENKEL );
    localStorage.setItem("HENKEL", HENKEL);
    LINDE.push( priceDiff_LINDE );
    localStorage.setItem("LINDE", LINDE);
    MERCK.push( priceDiff_MERCK );
    localStorage.setItem("MERCK", MERCK);
    SAP.push( priceDiff_SAP );
    localStorage.setItem("SAP", SAP);

    x_axis.push(timestamp);
    localStorage.setItem("x_axis", x_axis);
}
 function newpoint() {
    var len = BASF.length;
    if(len>1){
        BASF_delta.push(BASF[len-1]-BASF[len-2]);
        localStorage.setItem("BASF_delta", BASF_delta);

        HENKEL_delta.push(HENKEL[len-1]-HENKEL[len-2]);
        localStorage.setItem("HENKEL_delta", HENKEL_delta);

	LINDE_delta.push( LINDE[len-1]-  LINDE[len-2]);
        localStorage.setItem("LINDE_delta", LINDE_delta);

        MERCK_delta.push(MERCK[len-1]-MERCK[len-2]);
        localStorage.setItem("MERCK_delta", MERCK_delta);

	SAP_delta.push( SAP[len-1]-  SAP[len-2]);
        localStorage.setItem("SAP_delta", SAP_delta);
    }
} 

function renew()
{
    newrecord();
   // newpoint();
}

//update url every 0.3 seconds
$(function() {
    document.getElementById("table").style.display="";
    setInterval(renew(), 200);
    setInterval(function () {document.getElementById("search_button").click();}, 200);
});
*/

//window.setInterval( function () {renew()  }, 200);
//window.setInterval( function () {  document.getElementById("search_button").click()   }, 200);
