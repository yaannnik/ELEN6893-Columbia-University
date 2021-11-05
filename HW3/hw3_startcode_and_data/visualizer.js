var data = [80, 100, 56, 120, 180, 30, 40, 120, 160];
var svgWidth = 500, svgHeight = 300;

var barPaddingWidth = 5;
var textPaddingWidth = 2;
var barWidth = svgWidth / data.length - barPaddingWidth;
// The required padding between bars is 5px.
// The label must locate 2px above the middle of each bar.

var svg = d3.select('svg')
    .attr("width", svgWidth)
    .attr("height", svgHeight);

var barChart = svg.selectAll("rect")
    .data(data)
    .enter();

barChart.append("rect")
    .attr("class", "bar")
    .attr("height", function(d) {return d;})
    .attr("width", barWidth)
    .attr("transform", function(d, i) {
        return "translate(" + (barWidth + barPaddingWidth) * i 
                            + "," 
                            + (svgHeight - d)
                            + ")";
    })
    .attr("fill", "#CC6450");

barChart.append("text")
    .text(function(d) {return d;})
    .attr("transform", function(d, i) {
        return "translate(" + ((barWidth + barPaddingWidth) * i + barWidth / 2) 
                            + "," 
                            + (svgHeight - d - textPaddingWidth) 
                            + ")";
    })
    .style("text-anchor", "middle");