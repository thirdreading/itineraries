// Declarations
var Highcharts;
var optionSelected;
var dropdown = $('#option_selector');
var url = 'https://raw.githubusercontent.com/thirdreading/itineraries/develop/warehouse/data/themes.json';


// Menu data
$.getJSON(url, function (source) {

    $.each(source, function (key, entry) {
        dropdown.append($('<option></option>').attr('value', entry.name).text(entry.desc));
    });

    // Load the first Option by default
    var defaultOption = dropdown.find("option:first-child").val();
    optionSelected = dropdown.find("option:first-child").text();

    // Generate
    generateChart(defaultOption, optionSelected);

});


// Dropdown
dropdown.on('change', function (e) {

    $('#option_selector_title').remove();

    // Save name and value of the selected option
    optionSelected = this.options[e.target.selectedIndex].text;
    var valueSelected = this.options[e.target.selectedIndex].value;

    //Draw the Chart
    generateChart(valueSelected, optionSelected);

});




function generateChart(fileNameKey, fileNameLabel){


	$.getJSON(url, function(source){

		// Partitions
		var partitions = [];
		for (var i = 0; i < source.length; i += 1){
			partitions[i] = source[i].name;
		}


		// Indices
		var pa = partitions.indexOf(parseInt(fileNameKey));


		// Select
		var seriesOptions = [];
        seriesOptions = {
            name: source[pa].desc,
            data: source[pa].data
        };


		// Graphing
		Highcharts.chart('container', {

	        chart: {
	            zoomType: 'x',
	            type: 'timeline'
	        },

	        xAxis: {
	            type: 'datetime',
	            visible: false
	        },

	        yAxis: {
	            gridLineWidth: 0.5,
	            title: null,
	            labels: {
	                enabled: false
	            }
	        },

	        legend: {
	            enabled: false
	        },

	        title: {
	            text: 'Themes'
	        },

	        subtitle: {
	            text: 'Info source: <a href="https://www.gov.scot/collections/scottish-government-statistics/">Scottish Government Statistics</a>'
	        },

	        tooltip: {
	            style: {
	                width: 125
	            }
	        },

	        series: [{
	            dataLabels: {
	                allowOverlap: false,
	                format: '<span style="color:{point.color}">● </span><span style="font-weight: bold;" > ' +
	                    '{point.x:%d %b %Y}</span><br/>{point.label}'
	            },
	            marker: {
	                symbol: 'circle'
	            },
	            data: seriesOptions.data
	        }]

        });

	});

}





