/**
 * ConsoleUtils
 *
 * @author XJks
 * @description 控制台前端纯函数工具，避免在 Vue 实例中堆积无状态算法逻辑。
 */
(function (global) {
    'use strict';

    function toTrimmedString(value) {
        return value === null || typeof value === 'undefined' ? '' : String(value).trim();
    }

    function normalizeEndpoint(host, port) {
        var normalizedHost = toTrimmedString(host).toLowerCase();
        var normalizedPort = Number(port);
        return normalizedHost && normalizedPort > 0 ? normalizedHost + ':' + normalizedPort : '';
    }

    function findChildDir(node, name) {
        for (var i = 0; i < node.children.length; i++) {
            if (node.children[i].type === 'dir' && node.children[i].name === name) {
                return node.children[i];
            }
        }
        return null;
    }

    function sortTree(node) {
        node.children.sort(function (leftNode, rightNode) {
            if (leftNode.type !== rightNode.type) {
                return leftNode.type === 'dir' ? -1 : 1;
            }
            return leftNode.name.localeCompare(rightNode.name);
        });
        for (var i = 0; i < node.children.length; i++) {
            if (node.children[i].children) {
                sortTree(node.children[i]);
            }
        }
    }

    function buildTree(items) {
        var root = {name: '/', path: '/', type: 'dir', expanded: true, children: []};
        for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var key = item.key || '';
            var clean = key.charAt(0) === '/' ? key.substring(1) : key;
            var parts = clean ? clean.split('/') : [key];
            var current = root;
            var accumulated = key.charAt(0) === '/' ? '/' : '';
            for (var j = 0; j < parts.length; j++) {
                var part = parts[j] || '/';
                var isLast = j === parts.length - 1;
                accumulated += (accumulated && accumulated !== '/' ? '/' : '') + part;
                if (isLast) {
                    current.children.push({
                        name: part,
                        path: key,
                        key: key,
                        type: 'key',
                        item: item,
                        children: []
                    });
                } else {
                    var dir = findChildDir(current, part);
                    if (!dir) {
                        dir = {name: part, path: accumulated + '/', type: 'dir', expanded: true, children: []};
                        current.children.push(dir);
                    }
                    current = dir;
                }
            }
        }
        sortTree(root);
        return root;
    }

    function safeParseJson(jsonText) {
        try {
            return {ok: true, data: JSON.parse(jsonText)};
        } catch (error) {
            return {ok: false, error: error};
        }
    }

    global.consoleUtils = {
        normalizeEndpoint: normalizeEndpoint,
        buildTree: buildTree,
        safeParseJson: safeParseJson
    };
})(window);

