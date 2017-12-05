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



    exec(option_dt);
//    var timeTicket_filled = setInterval(function (){
//        exec(option);
//    },3000);

    function exec(option){
        $.ajax({
            dateType: "json",
            cache: false,
            type: "GET",
            url: '/api/highest_monthly_sales_by_category/Programming/10',
            async: false,
            success : function(data) {
                console.log(data);
                option_dt.options[0].series[0].data = data
                myChart.setOption(option_dt,true);
            }
        });


//        console.log(option_dt)
//        option_dt.options[0].series[0].data = [{name : 'Apple', value : 23}, {name : 'J', value : 13}]
////        option.series[0].data[0].value = (Math.random()*100).toFixed(2) - 0;
//        myChart.setOption(option_dt,true);
    }




}



            /*----------------- Chart 6 ------------------*/
if($("#gauge_chart_filled").length){

// Initialize after dom ready
        var myChart = echarts.init(document.getElementById('gauge_chart_filled'));

var option = {
    tooltip : {
        formatter: "{a} <br/>{b} : {c}%"
    },
    toolbox: {
        show : true,
        feature : {
            mark : {show: true},
            restore : {show: true},
            saveAsImage : {show: true}
        }
    },
    series : [
        {
            name:'',
            type:'gauge',
            startAngle: 180,
            endAngle: 0,
            center : ['50%', '90%'],
            radius : 200,
            /*axisLine: {
                lineStyle: {
                    width: 200
                }
            },*/
            axisLine: {
                            show: true,
                            lineStyle: {
                                color: [
                                    [0.2, '#536DFE'],
                                    [0.8, 'rgba(63,81,181,1)'],
                                    [1, '#E91E63']
                                ],
                                width:150
                            }
                        }  ,
            axisTick: {
                splitNumber: 10,
                length :12,
            },
            axisLabel: {
                formatter: function(v){
                    switch (v+''){
                        case '10': return 'Low';
                        case '50': return 'Medium';
                        case '90': return 'High';
                        default: return '';
                    }
                },
                textStyle: {
                    color: '#fff',
                    fontSize: 15,
                    fontWeight: 'bolder'
                }
            },
            pointer: {
                width:50,
                length: '90%',
                color: 'rgba(255, 255, 255, 0.8)'
            },
            title : {
                show : true,
                offsetCenter: [0, '-60%'],
                textStyle: {
                    color: '#fff',
                    fontSize: 30
                }
            },
            detail : {
                show : true,
                backgroundColor: 'rgba(0,0,0,0)',
                borderWidth: 0,
                borderColor: '#ccc',
                width: 100,
                height: 40,
                offsetCenter: [0, -40],
                formatter:'{value}%',
                textStyle: {
                    fontSize : 30
                }
            },
            data:[{value: 50, name: ''}]
        }
    ]


        /*var option = {

                tooltip : {
                    formatter: "{b} : {c}%"
                },
                toolbox: {
                    show : false,
                    feature : {
                        mark : {show: false},
                        restore : {show: false},
                        saveAsImage : {show: true}
                    }
                },
                series : [
                    {
                        name:'Server Load',
                        type:'gauge',
                        center: ['50%', '50%'],
                        radius: ['0%', '100%'],
                        axisLine: {
                            show: true,
                            lineStyle: {
                                color: [
                                    [0.2, '#536DFE'],
                                    [0.8, 'rgba(63,81,181,1)'],
                                    [1, '#E91E63']
                                ],
                                width: 10
                            }
                        }  ,
                        title: {
                            show : false,
                            offsetCenter: [0, '120%'],
                            textStyle: {
                                color: '#333',
                                fontSize : 15
                            }
                        }  ,
                        detail: {
                            show : true,
                            backgroundColor: 'rgba(0,0,0,0)',
                            borderWidth: 0,
                            borderColor: '#ccc',
                            width: 100,
                            height: 40,
                            offsetCenter: [0, '40%'],
                            formatter:'{value}%',
                            textStyle: {
                                color: 'auto',
                                fontSize : 20
                            }
                        },

                        data:[{value: 50, name: 'Server Load (MB)'}]
                    }
             ]*/
};

//myChart.setOption(option);
gauge_load_chart_filled(option);
var timeTicket_filled = setInterval(function (){
    gauge_load_chart_filled(option);
},1500);

function gauge_load_chart_filled(option){
    option.series[0].data[0].value = (Math.random()*100).toFixed(2) - 0;
    myChart.setOption(option,true);
}


//clearInterval(timeTicket_filled);


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
