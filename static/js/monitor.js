function filterText(queryText) {
  var objects = document.querySelectorAll('#object-tree > li > ul > li > ul > li > ul > li > div > span.tree-title');
  for (var i = 0; i < objects.length; i++) {
    if (objects[i].textContent.toLowerCase().indexOf(queryText.toLowerCase()) == -1) {
      $('#' + objects[i].parentNode.id).addClass('hidden');
    } else {
      $('#' + objects[i].parentNode.id).removeClass('hidden');
    };

    if (queryText === "") {
      $('#' + objects[i].parentNode.id).removeClass('hidden');
    };

  }
};

$(function() {
  function setupUpdater() {
    var input = document.querySelector('#query'),
      oldText = input.value,
      timeout = null;

    /* handleChange is called 300ms after the user stops
       typing. */
    function handleChange() {
      var queryText = input.value;
      filterText(queryText)
    }

    /* eventHandler is called on keyboard and mouse events.
       If there is a pending timeout, it cancels it.
       It sets a timeout to call handleChange in 300ms. */
    function eventHandler() {
      if (timeout) clearTimeout(timeout);
      timeout = setTimeout(handleChange, 300);
    }

    input.onkeydown = input.onkeyup = input.onclick = eventHandler;
  }

  setupUpdater();
});


var funcs = {
  refreshMonData: function(env) {
    $('#table_'+env).datagrid('reload');
  },
  pollMonData: function(env) {
    $.get("poll_mon_data?env="+env, function(data) {
      console.log((new Date()).toISOString() + ' -- ', data);
    });
  },

};


// Switch buttons initiation
var initSwitch = function(switch_id) {
  var switch_status = false;
  if (localStorage.getItem(switch_id) == 'true') {
    switch_status = true;
  };
  $('#' + switch_id).switchbutton({
    checked: switch_status,
    onChange: function(checked) {
      localStorage.setItem(switch_id, checked);
      // $.get("switch?id="+switch_id+"&status="+checked, function(data) {
      //   console.log((new Date()).toISOString() + ' -- ', data);
      // });
      
    }
  })
}

initSwitch('switch_dev');
initSwitch('switch_qa');
initSwitch('switch_prd');

function createTable(env){
  var id_ = '#table_' + env;
  var table_ = $(id_).datagrid({
    url: 'monitor_data.json?env='+env,
    singleSelect:true,
    pageSize:20,
    onDblClickRow: function(index,row){
      var row = $(id_).datagrid('getSelected');
      if (row){
          $.messager.alert('Error Message', row.error);
      }
    },
    method:'get',
    view: detailview,
    detailFormatter:function(index,row){
        return '<div class="ddv" style="padding:5px 0"></div>';
    },
    onExpandRow: function(index,row){
        var ddv = $(this).datagrid('getRowDetail',index).find('div.ddv');
        ddv.panel({
            height:500,
            border:false,
            cache:false,
            // href:'datagrid21_getdetail.php?itemid='+row.itemid,
            href:'get_session_detail.stat?combo='+ row.combo + '&env='+env,
            onLoad:function(){
                $('#dg').datagrid('fixDetailRowHeight',index);
            }
        });
        $('#dg').datagrid('fixDetailRowHeight',index);
    },
    columns: [
      [{
        field: 'folder',
        title: 'FOLDER',
        sortable: true,
        width: 150
      }, {
        field: 'workflow',
        title: 'WORKFLOW',
        sortable: true,
        width: 200
      }, {
        field: 'session',
        title: 'SESSION',
        sortable: true,
        width: 200
      }, {
        field: 'mapping',
        title: 'MAPPING',
        sortable: true,
        width: 200
      }, {
        field: 'start',
        title: 'START',
        sortable: true,
        width: 138
      }, {
        field: 'duration',
        title: 'DUR.',
        width: 40
      }, {
        field: 'success',
        title: 'Success',
        width: 75,
        styler: function(value, row, index) {
          if (value == 'No') {
            return 'background-color:#ffee00;color:red;';
          }
          if (value == 'Running') {
            return 'background-color:#ccffff;';
          }
        }
      }, {
        field: 'error',
        title: 'Execution Message',
        width: 250
      }, {
        field: 'combo',
        title: 'Combo',
        width: 2
      }]
    ]
  });

  table_.datagrid('enableFilter', [{
                  field:'success',
                  type:'combobox',
                  options:{
                      panelHeight:'auto',
                      data:[{value:'',text:'All'},{value:'Running',text:'Running'},{value:'Yes',text:'Yes'},{value:'No',text:'No'}],
                      onChange:function(value){
                          if (value == ''){
                              table_.datagrid('removeFilterRule', 'success');
                          } else {
                              table_.datagrid('addFilterRule', {
                                  field: 'success',
                                  op: 'equal',
                                  value: value
                              });
                          }
                          table_.datagrid('doFilter');
                      }
                  }
              }]);
  return table_;
};

createTable('dev');
createTable('qa');
createTable('prd');


function monitor_poll_data()
{
    if (localStorage.getItem('switch_dev') == 'true') {
      funcs['pollMonData']('dev');
      funcs['refreshMonData']('dev');
    };
    if (localStorage.getItem('switch_qa') == 'true') {
      funcs['pollMonData']('qa');
      funcs['refreshMonData']('qa');
    };
    if (localStorage.getItem('switch_prd') == 'true') {
      funcs['pollMonData']('prd');
      funcs['refreshMonData']('prd');
    };
    setTimeout(monitor_poll_data, 10000);
}

monitor_poll_data();

// event handler
var evtSrc = new EventSource("/subscribe");

evtSrc.addEventListener('message', function(e) {

  if (e.data in funcs) {
    console.log((new Date()).toISOString() + ' >> EXEC ', e.data);
    funcs[e.data]();
  } else {
    console.log((new Date()).toISOString() + ' -- ', e.data);
  }
});


// add a new tab panel
var index = 0;

function addPanel() {
  index++;
  $('#tt').tabs('add', {
    title: '[DEV.SRC] PPADMIN.JOJJO',
    // title: '[DEV.TGT] PPADMIN.JOJJO',
    // title: '[DEV.MAP] PPADMIN.JOJJO',
    // title: '[DEV.SES] PPADMIN.JOJJO',
    // title: '[DEV.WKF] PPADMIN.JOJJO',
    content: '<div style="padding:10px">Content' + index + '</div>',
    closable: true
  });
}

function removePanel() {
  var tab = $('#tt').tabs('getSelected');
  if (tab) {
    var index = $('#tt').tabs('getTabIndex', tab);
    $('#tt').tabs('close', index);
  }
}

function collapseAll() {
  $('#object-tree').tree('collapseAll');
}

function expandAll() {
  $('#object-tree').tree('expandAll');
}

function expandTo() {
  var node = $('#object-tree').tree('find', 113);
  $('#object-tree').tree('expandTo', node.target).tree('select', node.target);
}

function getSelected() {
  var node = $('#object-tree').tree('getSelected');
  if (node) {
    var s = node.text;
    if (node.attributes) {
      s += "," + node.attributes.p1 + "," + node.attributes.p2;
    }
    alert(s);
  }
}
