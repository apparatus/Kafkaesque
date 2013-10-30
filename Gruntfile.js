/*jshint globalstrict: true, white: false */
/*global module */
'use strict';

module.exports = function (grunt) {
  require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);

  grunt.initConfig({
    jshint: {
      options: {
        jshintrc: '.jshintrc'
      },
      all: [
        'Gruntfile.js',
        'test/{,*/}*.js',
        'lib/{,*/}*.js'
      ]
    },
    mochaTest: {
      test: {
        options: {
          reporter: 'spec'
        },
        src: ['test/unit/**/*Test.js']
      }
    },
    mochacov: {
      options: {
        reporter: 'html-cov'
      },
      all: ['test/unit/**/*.js']
    }
  });

  grunt.registerTask('coverage', [
    'mochacov'
  ]);

  grunt.registerTask('test', [
    'mochaTest',
  ]);

  grunt.registerTask('build', [
    'jshint',
    'test',
    'coverage'
  ]);

  grunt.registerTask('travis', [
    'test'
  ]);
};

