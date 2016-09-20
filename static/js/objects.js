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

$(function () {
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
  refreshObjectTree: function () {
    // if Date() < now() - 10 sec:
    var now = new Date();
    var last_refresh = new Date(localStorage.getItem("last_refresh"));
    if (last_refresh.valueOf() < now.valueOf() - 10 * 1000) {
      localStorage.setItem("last_refresh", Date().toString());
      $('#object-tree').tree({
        url: 'object_tree.json',
        lines: true,
        formatter: function (node) {
          var s = node.text;
          if (node.children /*&& node.parent.text != 'DEV'*/) {
            s += '&nbsp;<span style=\'color:blue\'>(' + node.children.length + ')</span>';
          }
          return s;
        }
      });
      localStorage.setItem("last_refresh", Date().toString());
    };
  },
  refresh: function () {
    $.get("refresh", function (data) {
      console.log("Initiated folder refresh: " + data);
    });
  },

};

funcs['refreshObjectTree'](); // passive refresh tree on load



// event handler
var evtSrc = new EventSource("/subscribe");

evtSrc.addEventListener('message', function (e) {
  
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

