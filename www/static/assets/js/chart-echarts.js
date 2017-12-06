/**
 * @Package: Complete Admin Responsive Theme
 * @Since: Complete Admin 1.0
 * This file is part of Complete Admin Responsive Theme.
 */


jQuery(function($) {

    'use strict';

    var CMPLTADMIN_SETTINGS = window.CMPLTADMIN_SETTINGS || {};




    /*--------------------------------
         Window Based Layout
     --------------------------------*/
    CMPLTADMIN_SETTINGS.dashboardEcharts = function() {






        /*--------------- Chart 3 -------------*/
    if($("#platform_type_dates").length){
    var myChart = echarts.init(document.getElementById('platform_type_dates'));

    var idx = 1;
    var option_dt = {

        timeline : {
            show: false,
            data : ['06-16','05-16','04-16'],
            label : {
                formatter : function(s) {
                    return s.slice(0, 5);
                }
            },
            x:10,
            y:null,
            x2:10,
            y2:0,
            width:250,
            height:50,
            backgroundColor:"rgba(0,0,0,0)",
            borderColor:"#eaeaea",
            borderWidth:0,
            padding:5,
            controlPosition:"left",
            autoPlay:false,
            loop:false,
            playInterval:2000,
            lineStyle:{
                width:1,
                color:"#bdbdbd",
                type:""
            },

        },

        options : [
                    {
                        color: ['#3F51B5','#303F9F','#1A237E','#9FA8DA','#7986CB','#C5CAE9'],
                        title : {
                            text: '',
                            subtext: ''
                        },
                        tooltip : {
                            trigger: 'item',
                            formatter: "{a} <br/>{b} : {c} ({d}%)"
                        },
                        legend: {
                            show: false,
                            x: 'left',
                            orient:'vertical',
                            padding: 0,
                            data:['Apple','Windows','Linux','Android','Others']
                        },
                        toolbox: {
                            show : true,
                            color : ['#bdbdbd','#bdbdbd','#bdbdbd','#bdbdbd'],
                            feature : {
                                mark : {show: false},
                                dataView : {show: false, readOnly: true},
                                magicType : {
                                    show: true,
                                    type: ['pie', 'funnel'],
                                    option: {
                                        funnel: {
                                            x: '10%',
                                            width: '80%',
                                            funnelAlign: 'center',
                                            max: 50
                                        },
                                        pie: {
                                            roseType : 'none',
                                        }
                                    }
                                },
                                restore : {show: false},
                                saveAsImage : {show: true}
                            }
                        },
                        series : [
                            {
                                name:'06-16',
                                type:'pie',
                                radius : [20, '80%'],
                                roseType : 'radius',
                                center: ['50%', '45%'],
                                width: '50%',       // for funnel
                                itemStyle : {
                                    normal : { label : { show : true }, labelLine : { show : true } },
                                    emphasis : { label : { show : false }, labelLine : {show : false} }
                                },
                                data:[]
                            }
                        ]
                    },

        ] // end options object
    };

//    myChart.setOption(option_dt);

    if($("#btn1").length) {
        $("#btn1").click (
            function() {
                var value1 = $("#input1").val()
                var value2 = $("#input2").val()
                exec(option_dt, value1, value2);
            }
        );
    }




//    var timeTicket_filled = setInterval(function (){
//        exec(option);
//    },3000);

    function exec(option, value1, value2){
        $.ajax({
            dateType: "json",
            cache: false,
            type: "GET",
            url: '/api/highest_monthly_sales_by_category/' + value1 + '/' + value2,
            async: true,
            success : function(data) {
                console.log(data);
                option_dt.options[0].series.name = value1;
                option_dt.options[0].series[0].data = data;
                myChart.setOption(option_dt,true);
            }
        });


//        console.log(option_dt)
//        option_dt.options[0].series[0].data = [{name : 'Apple', value : 23}, {name : 'J', value : 13}]
////        option.series[0].data[0].value = (Math.random()*100).toFixed(2) - 0;
//        myChart.setOption(option_dt,true);
    }




}


    if($("#pie_chart2").length){
    var myChart2 = echarts.init(document.getElementById('pie_chart2'));

    var idx = 1;
    var option_dt2 = {

        timeline : {
            show: false,
            data : ['06-16','05-16','04-16'],
            label : {
                formatter : function(s) {
                    return s.slice(0, 5);
                }
            },
            x:10,
            y:null,
            x2:10,
            y2:0,
            width:250,
            height:50,
            backgroundColor:"rgba(0,0,0,0)",
            borderColor:"#eaeaea",
            borderWidth:0,
            padding:5,
            controlPosition:"left",
            autoPlay:false,
            loop:false,
            playInterval:2000,
            lineStyle:{
                width:1,
                color:"#bdbdbd",
                type:""
            },

        },

        options : [
                    {
                        color: ['#3F51B5','#303F9F','#1A237E','#9FA8DA','#7986CB','#C5CAE9'],
                        title : {
                            text: '',
                            subtext: ''
                        },
                        tooltip : {
                            trigger: 'item',
                            formatter: "{a} <br/>{b} : {c} ({d}%)"
                        },
                        legend: {
                            show: false,
                            x: 'left',
                            orient:'vertical',
                            padding: 0,
                            data:['Apple','Windows','Linux','Android','Others']
                        },
                        toolbox: {
                            show : true,
                            color : ['#bdbdbd','#bdbdbd','#bdbdbd','#bdbdbd'],
                            feature : {
                                mark : {show: false},
                                dataView : {show: false, readOnly: true},
                                magicType : {
                                    show: true,
                                    type: ['pie', 'funnel'],
                                    option: {
                                        funnel: {
                                            x: '10%',
                                            width: '80%',
                                            funnelAlign: 'center',
                                            max: 50
                                        },
                                        pie: {
                                            roseType : 'none',
                                        }
                                    }
                                },
                                restore : {show: false},
                                saveAsImage : {show: true}
                            }
                        },
                        series : [
                            {
                                name:'06-16',
                                type:'pie',
                                radius : [20, '80%'],
                                roseType : 'radius',
                                center: ['50%', '45%'],
                                width: '50%',       // for funnel
                                itemStyle : {
                                    normal : { label : { show : true }, labelLine : { show : true } },
                                    emphasis : { label : { show : false }, labelLine : {show : false} }
                                },
                                data:[]
                            }
                        ]
                    },

        ] // end options object
    };

//    myChart.setOption(option_dt);

    if($("#btn2").length) {
        $("#btn2").click (
            function() {
                var value1 = $("#input3").val()
                var value2 = $("#input4").val()
                exec2(option_dt2, value1, value2);
            }
        );
    }

    function exec2(option, value1, value2){
        $.ajax({
            dateType: "json",
            cache: false,
            type: "GET",
            url: '/api/top_sales_category/' + value1 + '/' + value2,
            async: true,
            success : function(data) {
                console.log(data);
                option_dt.options[0].series.name = value1;
                option_dt.options[0].series[0].data = data;
                myChart2.setOption(option_dt,true);
            }
        });


//        console.log(option_dt)
//        option_dt.options[0].series[0].data = [{name : 'Apple', value : 23}, {name : 'J', value : 13}]
////        option.series[0].data[0].value = (Math.random()*100).toFixed(2) - 0;
//        myChart.setOption(option_dt,true);
    }




}



    }



    /******************************
     initialize respective scripts 
     *****************************/
    $(document).ready(function() {
        CMPLTADMIN_SETTINGS.dashboardEcharts();
    });

    $(window).resize(function() {
        CMPLTADMIN_SETTINGS.dashboardEcharts();
    });

    $(window).load(function() {});

});
