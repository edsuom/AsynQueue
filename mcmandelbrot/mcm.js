function buildUrl(url, parameters) {
    // Copied from Michael's answer at
    // http://stackoverflow.com/questions/316781/
    //   how-to-build-query-string-with-javascript
    var qs = "";
    for(var key in parameters) {
	var value = parameters[key];
	qs += encodeURIComponent(key) + "=" + encodeURIComponent(value) + "&";
    }
    if (qs.length > 0){
	qs = qs.substring(0, qs.length-1); //chop off last "&"
	url = url + "?" + qs;
    }
    return url;
}
function getParams() {
    var params = {'cr':"", 'ci':"", 'crpm':""};
    for (var name in params) {
	params[name] = document.getElementById(name).value;
    }
    params.N = document.getElementById('image').clientWidth;
    return params
}
function updateImage(params) {
    if (params === undefined) {
	var params = getParams();
    } else {
	for (var name in params) {
	    document.getElementById(name).value = params[name];
	}
    }
    var img = document.getElementById('mandelbrot');
    img.src = buildUrl("/image.png", params);
}
function xy(event) {
    var p = {};
    var params = getParams();
    var image = document.getElementById('mandelbrot');
    var x = (event.clientX - image.offsetLeft - 5) / image.clientWidth;
    var y = (event.clientY - image.offsetTop) / image.clientHeight;
    p.crpm = Number(params.crpm);
    p.cr = p.crpm * (2*x - 1) + Number(params.cr);
    p.ci = p.crpm * (1 - 2*y) + Number(params.ci);
    return p
}
function zoomIn(event) {
    var params = xy(event);
    params.crpm = 0.2 * params.crpm;
    updateImage(params);
}
function zoomOut() {
    var params = getParams();
    params.crpm = 5 * params.crpm;
    updateImage(params);
}
function hover(event) {
    var params = xy(event)
    var message = "(" + params.cr + ", " + params.ci + "), +/-" + params.crpm;
    document.getElementById('hover').innerHTML = message;
}
