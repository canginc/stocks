
var coffee = localStorage.getItem("coffee");
if(coffee==null) coffee = [];
else coffee = domStringToList(coffee);

var coffee_delta = localStorage.getItem("coffee_delta");
if(coffee_delta==null) coffee_delta = [0];
else coffee_delta = domStringToList(coffee_delta);

var tea = localStorage.getItem("tea");
if(tea==null) tea = [];
else tea = domStringToList(tea);

var tea_delta = localStorage.getItem("tea_delta");
if(tea_delta==null) tea_delta = [0];
else tea_delta = domStringToList(tea_delta);

var milk = localStorage.getItem("milk");
if(milk==null) milk = [];
else milk = domStringToList(milk);

var milk_delta = localStorage.getItem("milk_delta");
if(milk_delta==null) milk_delta = [0];
else milk_delta = domStringToList(milk_delta);

var soda = localStorage.getItem("soda");
if(soda==null) soda = [];
else soda = domStringToList(soda);

var soda_delta = localStorage.getItem("soda_delta");
if(soda_delta==null) soda_delta = [0];
else soda_delta = domStringToList(soda_delta);

var juice = localStorage.getItem("juice");
if(juice==null) juice = [];
else juice = domStringToList(juice);

var juice_delta = localStorage.getItem("juice_delta");
if(juice_delta==null) juice_delta = [0];
else juice_delta = domStringToList(juice_delta);

var wine = localStorage.getItem("wine");
if(wine==null) wine = [];
else wine = domStringToList(wine);

var wine_delta = localStorage.getItem("wine_delta");
if(wine_delta==null) wine_delta = [0];
else wine_delta = domStringToList(wine_delta);

var beer = localStorage.getItem("beer");
if(beer==null) beer = [];
else beer = domStringToList(beer);

var beer_delta = localStorage.getItem("beer_delta");
if(beer_delta==null) beer_delta = [0];
else beer_delta = domStringToList(beer_delta);

var liquor = localStorage.getItem("liquor");
if(liquor==null) liquor = [];
else liquor = domStringToList(liquor);

var liquor_delta = localStorage.getItem("liquor_delta");
if(liquor_delta==null) liquor_delta = [0];
else liquor_delta = domStringToList(liquor_delta);

var x_axis = [];
var x_axis = localStorage.getItem("x_axis");
if(x_axis==null) x_axis = [];
else x_axis = x_axis.split(",");

if(document.getElementById('count_coffee')!=null){
    var coffee_count = parseInt(document.getElementById('count_coffee').innerHTML);
    var tea_count = parseInt(document.getElementById('count_tea').innerHTML);
    var milk_count = parseInt(document.getElementById('count_milk').innerHTML);
    var soda_count = parseInt(document.getElementById('count_soda').innerHTML);
    var juice_count = parseInt(document.getElementById('count_juice').innerHTML);
    var wine_count = parseInt(document.getElementById('count_wine').innerHTML);
    var beer_count = parseInt(document.getElementById('count_beer').innerHTML);
    var liquor_count = parseInt(document.getElementById('count_liquor').innerHTML);
    var sum = coffee_count + tea_count + milk_count + soda_count + juice_count + wine_count + beer_count + liquor_count;
}

var current = new Date();
var h = fixTimeFormat(current.getHours());
var m = fixTimeFormat(current.getMinutes());
var s = fixTimeFormat(current.getSeconds());
var timestamp = h + ":" + m + ":" +s; 


var myPieChart = Highcharts.chart('piechart', {
    chart: {plotBackgroundColor: null,plotBorderWidth: null,plotShadow: false,type: 'pie'},
    title: {text: 'Estimated Drink Market Shares'},
    tooltip: {pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'},
    plotOptions: {
    pie: {allowPointSelect: true,cursor: 'pointer',
    dataLabels: {enabled: true,format: "<b>{point.name}</b>: {point.percentage:.1f} %",
    style: {color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'}}}},
    series: [{
    name: 'Drinks',
    colorByPoint: true,
    data: [{name: 'Coffee',y: coffee_count/sum, x: coffee_count}, 
           {name: 'Tea',y: tea_count/sum,sliced: true,selected: true}, 
           {name: 'Milk',y: milk_count/sum}, 
           {name: 'Soda',y: soda_count/sum}, 
           {name: 'Juice',y: juice_count/sum}, 
           {name: 'Wine',y: wine_count/sum}, 
           {name: 'Beer',y: beer_count/sum}, 
           {name: 'Liquor',y: liquor_count/sum}]}]
});

var myLineChart = Highcharts.chart('linechart', {
    chart: {type: 'spline'},
    title: {text: 'Drink Trend on Twitter'},
    xAxis: {categories: x_axis},
    yAxis: {title: {text: 'Trend'}},
    series: [{name: 'Coffee',data: coffee_delta},
             {name: 'Tea',data: tea_delta},
             {name: 'Milk',data: milk_delta},
             {name: 'Soda',data: soda_delta},
             {name: 'Juice',data: juice_delta},
             {name: 'Wine',data: wine_delta},
             {name: 'Beer',data: beer_delta},
             {name: 'Liquor',data: liquor_delta}]
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
    coffee.push(coffee_count);
    localStorage.setItem("coffee", coffee);
    tea.push(tea_count);
    localStorage.setItem("tea", tea);
    milk.push(milk_count);
    localStorage.setItem("milk", milk);
    soda.push(soda_count);
    localStorage.setItem("soda", soda);
    juice.push(juice_count);
    localStorage.setItem("juice", juice);
    wine.push(wine_count);
    localStorage.setItem("wine", wine);
    beer.push(beer_count);
    localStorage.setItem("beer", beer);
    liquor.push(liquor_count);
    localStorage.setItem("liquor", liquor);
    x_axis.push(timestamp);
    localStorage.setItem("x_axis", x_axis);
}

function newpoint() {
    var len = coffee.length;
    if(len>1){
        coffee_delta.push(coffee[len-1]-coffee[len-2]);
        localStorage.setItem("coffee_delta", coffee_delta);
        tea_delta.push(tea[len-1]-tea[len-2]);
        localStorage.setItem("tea_delta", tea_delta);
        milk_delta.push(milk[len-1]-milk[len-2]);
        localStorage.setItem("milk_delta", milk_delta);
        soda_delta.push(soda[len-1]-soda[len-2]);
        localStorage.setItem("soda_delta", soda_delta);
        juice_delta.push(juice[len-1]-juice[len-2]);
        localStorage.setItem("juice_delta", juice_delta);
        wine_delta.push(wine[len-1]-wine[len-2]);
        localStorage.setItem("wine_delta", wine_delta);
        beer_delta.push(beer[len-1]-beer[len-2]);
        localStorage.setItem("beer_delta", beer_delta);
        liquor_delta.push(liquor[len-1]-liquor[len-2]);
        localStorage.setItem("liquor_delta", liquor_delta);
    }
}

function renew()
{
    newrecord();
    newpoint();
}

//update url every 300s
$(function() {
    document.getElementById("table").style.display="";
    setInterval(renew(), 300000);
    setInterval(function () {document.getElementById("search_button").click();}, 300000);
});

