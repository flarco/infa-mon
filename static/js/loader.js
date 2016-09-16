// add a new tab panel

var index = 0;
function addPanel(){
    index++;
    $('#tt').tabs('add',{
        title: '[DEV.SRC] PPADMIN.JOJJO',
        // title: '[DEV.TGT] PPADMIN.JOJJO',
        // title: '[DEV.MAP] PPADMIN.JOJJO',
        // title: '[DEV.SES] PPADMIN.JOJJO',
        // title: '[DEV.WKF] PPADMIN.JOJJO',
        content: '<div style="padding:10px">Content'+index+'</div>',
        closable: true
    });
}
function removePanel(){
    var tab = $('#tt').tabs('getSelected');
    if (tab){
        var index = $('#tt').tabs('getTabIndex', tab);
        $('#tt').tabs('close', index);
    }
}