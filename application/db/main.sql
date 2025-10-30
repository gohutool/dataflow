/*
 Navicat Premium Data Transfer

 Source Server         : etcdv3
 Source Server Type    : SQLite
 Source Server Version : 3030001
 Source Schema         : main

 Target Server Type    : SQLite
 Target Server Version : 3030001
 File Encoding         : 65001

 Date: 30/10/2025 19:59:23
*/

PRAGMA foreign_keys = false;

-- ----------------------------
-- Table structure for t_config
-- ----------------------------
DROP TABLE IF EXISTS "t_config";
CREATE TABLE "t_config" (
  "id" TEXT NOT NULL,
  "node_name" TEXT,
  "node_host" TEXT,
  "node_port" TEXT,
  "node_token" TEXT,
  "node_demo" TEXT,
  "createtime" text,
  "updatetime" TEXT,
  PRIMARY KEY ("id")
);

-- ----------------------------
-- Records of t_config
-- ----------------------------
INSERT INTO "t_config" VALUES ('8CE8FB3C-E5BB-46F6-8046-B8CE444428DE', 'localhost', 'localhost', '12379', NULL, NULL, '2025-10-25 19:42:10', '2025-10-25 19:42:10');
INSERT INTO "t_config" VALUES ('A424FA03-75F8-4008-AF4C-11FBD4B802F2', '新建etcd连接名', 'localhost', '12379', NULL, '新建etcd连接名新建etcd连接名新建etcd连接名', '2025-10-29 23:00:25', '2025-10-30 18:30:14');

-- ----------------------------
-- Table structure for t_group
-- ----------------------------
DROP TABLE IF EXISTS "t_group";
CREATE TABLE "t_group" (
  "config_id" text NOT NULL,
  "group_content" text,
  "createtime" TEXT,
  "updatetime" TEXT,
  PRIMARY KEY ("config_id")
);

-- ----------------------------
-- Records of t_group
-- ----------------------------
INSERT INTO "t_group" VALUES ('A424FA03-75F8-4008-AF4C-11FBD4B802F2', '[{"group_id":"EF644FAD-89DF-42CD-9FE3-9804F3FE2080","db_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","group_name":"test111","group_prefix":"testaaaaaa","group_demo":"","id":"EF644FAD-89DF-42CD-9FE3-9804F3FE2080","node_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","createtime":"2025-10-30 18:57:11"},{"folder_id":"3378549B-FB97-4B65-AAAB-28165C9B2E97","db_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","folder_name":"testaaa","folder_demo":"testaaa","id":"3378549B-FB97-4B65-AAAB-28165C9B2E97","node_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","createtime":"2025-10-30 19:51:15","group":[]}]', '2025-10-30 18:56:48', '2025-10-30 19:53:43');

-- ----------------------------
-- Table structure for t_search
-- ----------------------------
DROP TABLE IF EXISTS "t_search";
CREATE TABLE "t_search" (
  "config_id" text NOT NULL,
  "search_id" text NOT NULL,
  "search_name" TEXT,
  "search_content" TEXT,
  "createtime" TEXT,
  "updatetime" TEXT
);

-- ----------------------------
-- Records of t_search
-- ----------------------------
INSERT INTO "t_search" VALUES ('A424FA03-75F8-4008-AF4C-11FBD4B802F2', '642227F2-D6A6-4398-AD28-52D93A5E05A3', 'test', '{"ignore_key":"1","min_create_revision":"","max_create_revision":"","min_mod_revision":"","max_mod_revision":"","count":"","sort_target":"","sort_order":"","node_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","search_name":"test","id":"642227F2-D6A6-4398-AD28-52D93A5E05A3","createtime":"2025-10-30 18:32:50","db_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2"}', '2025-10-30 18:32:50', NULL);
INSERT INTO "t_search" VALUES ('A424FA03-75F8-4008-AF4C-11FBD4B802F2', '0C7E84B7-E447-4080-8288-8169F088E5E1', 'test1111', '{"ignore_key":"1","min_create_revision":"","max_create_revision":"","min_mod_revision":"","max_mod_revision":"","count":"","key_only":"1","sort_target":"","sort_order":"","node_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2","search_name":"test1111","id":"0C7E84B7-E447-4080-8288-8169F088E5E1","updatetime":"2025-10-30 18:55:58","db_id":"A424FA03-75F8-4008-AF4C-11FBD4B802F2"}', '2025-10-30 18:33:53', '2025-10-30 18:55:58');

-- ----------------------------
-- Table structure for t_user
-- ----------------------------
DROP TABLE IF EXISTS "t_user";
CREATE TABLE "t_user" (
  "username" text NOT NULL,
  "password" TEXT,
  PRIMARY KEY ("username")
);

-- ----------------------------
-- Records of t_user
-- ----------------------------
INSERT INTO "t_user" VALUES ('ginghan', '$2b$10$FqiR1uD9qtf72lYYUA.zbeho1KMe8c5qjqSay01DXyA7FsLH8E29K');

PRAGMA foreign_keys = true;
