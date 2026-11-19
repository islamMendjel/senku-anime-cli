# Maintainer: Senku <senku@archlinux.org>
pkgname=senku-anime-cli-git
_pkgname=senku-anime-cli
pkgver=0.1.0.r0.g0000000
pkgrel=1
pkgdesc="Lightning-fast, zero-bloat Arabic-subbed anime CLI streamer and multi-threaded downloader"
arch=('x86_64' 'aarch64')
url="https://github.com/senku/senku-anime-cli"
license=('GPL-3.0-only')
depends=('mpv' 'gcc-libs')
makedepends=('cargo' 'git')
optdepends=(
    'fzf: interactive fuzzy search selector'
    'kitty: native Kitty video output (--kitty) support'
)
provides=("${_pkgname}" 'sac')
conflicts=("${_pkgname}")
source=("${_pkgname}::git+${url}.git")
sha256sums=('SKIP')

pkgver() {
    cd "${srcdir}/${_pkgname}"
    if git describe --long --tags --abbrev=7 >/dev/null 2>&1; then
        git describe --long --tags --abbrev=7 | sed 's/^v//;s/\([^-]*-g\)/r\1/;s/-/./g'
    else
        printf "0.1.0.r%s.g%s" "$(git rev-list --count HEAD)" "$(git rev-parse --short=7 HEAD)"
    fi
}

prepare() {
    cd "${srcdir}/${_pkgname}"
    export RUSTUP_TOOLCHAIN=stable
    cargo fetch --locked --target "$(rustc -vV | sed -n 's/host: //p')"
}

build() {
    cd "${srcdir}/${_pkgname}"
    export RUSTUP_TOOLCHAIN=stable
    export CARGO_TARGET_DIR=target
    cargo build --frozen --release --bin senku-anime-cli
}

check() {
    cd "${srcdir}/${_pkgname}"
    export RUSTUP_TOOLCHAIN=stable
    export CARGO_TARGET_DIR=target
    cargo test --frozen --release
}

package() {
    cd "${srcdir}/${_pkgname}"
    install -Dm755 "target/release/${_pkgname}" "${pkgdir}/usr/bin/${_pkgname}"
    ln -s "${_pkgname}" "${pkgdir}/usr/bin/sac"
    install -Dm644 LICENSE "${pkgdir}/usr/share/licenses/${pkgname}/LICENSE"
    install -Dm644 README.md "${pkgdir}/usr/share/doc/${pkgname}/README.md"
}
