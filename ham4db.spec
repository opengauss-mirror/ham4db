%define rpm_name ham4db
%define rpm_version 1.0.3
%define build_time 20200818100000
%define project_path *

Name:				%rpm_name
Version:			%rpm_version
Release:			%{build_time}%{?dist}
Provides:			ham4db
Group:				Sangfor
License:			GPLv2
URL:				http://www.sangfor.com.cn/
Summary:			%rpm_name
BuildRequires:                  gcc-c++
BuildRequires:                  go

%description
%rpm_name

%define __os_install_post %{nil}

%install
#export QA_SKIP_BUILD_ROOT=1
export INSTALLDIR=${RPM_BUILD_ROOT}

#
cp -rf ${SOURCE_DIR}/%{project_path} %{_builddir}
rm -rf ${RPM_BUILD_ROOT}/sf/bin
mkdir -p ${RPM_BUILD_ROOT}/sf/bin
mkdir -p ${RPM_BUILD_ROOT}/sf/etc

cd %{_builddir}

export GOPATH=%{_builddir}
mkdir -p src/gitee.com/opengauss/ham4db
tar -czvf ham4db.tar.gz *
cp -r ham4db.tar.gz src/gitee.com/opengauss/ham4db
cd src/gitee.com/opengauss/ham4db
tar -xzvf ham4db.tar.gz
GO111MODULE=auto go build -o ham4db go/app/main.go

cp ham4db ${RPM_BUILD_ROOT}/sf/bin
cp %{_builddir}/conf/ham4db.conf.json  ${RPM_BUILD_ROOT}/sf/etc

cd %{_builddir}
rm -rf src/gitee.com/opengauss/ham4db

%files
%defattr(-,root,root)

/sf/bin
/sf/etc
